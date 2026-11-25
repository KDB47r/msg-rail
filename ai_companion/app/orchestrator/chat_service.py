import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator

from app.llm.base import LLMProvider, Message, StreamEvent, Usage
from app.llm.errors import map_provider_error
from app.orchestrator.events import SSEEvent

logger = logging.getLogger(__name__)


def _usage_dict(usage: Usage | None) -> dict[str, int] | None:
    if usage is None:
        return None
    return {
        "input_tokens": usage.input_tokens,
        "output_tokens": usage.output_tokens,
        "cached_tokens": usage.cached_tokens,
    }


async def stream_turn(
    provider: LLMProvider,
    messages: list[Message],
    *,
    conversation_id: str,
    message_id: str,
    run_id: str,
    created_at: str,
    heartbeat_seconds: float,
) -> AsyncIterator[SSEEvent | None]:
    """Yield domain events for one chat turn.

    ``None`` means "send a heartbeat comment". Provider errors are converted to
    a stable SSE ``error`` event, so the HTTP response itself stays 200 once the
    stream has started.
    """

    yield SSEEvent(
        "message_start",
        {
            "conversation_id": conversation_id,
            "message_id": message_id,
            "run_id": run_id,
            "created_at": created_at,
        },
    )

    queue: asyncio.Queue[tuple[str, object]] = asyncio.Queue(maxsize=100)

    async def producer() -> None:
        cancelled = False
        try:
            async for event in provider.stream(messages):
                await queue.put(("event", event))
        except asyncio.CancelledError:
            cancelled = True
            raise
        except Exception as exc:  # noqa: BLE001 - boundary converts errors to SSE events
            logger.exception("provider stream failed run_id=%s", run_id)
            await queue.put(("error", exc))
        finally:
            if not cancelled:
                await queue.put(("done", None))

    task = asyncio.create_task(producer(), name=f"llm-{run_id}")
    usage: Usage | None = None
    finish_reason = "stop"

    try:
        while True:
            try:
                if heartbeat_seconds > 0:
                    item = await asyncio.wait_for(
                        queue.get(),
                        timeout=heartbeat_seconds,
                    )
                else:
                    item = await queue.get()
            except asyncio.TimeoutError:
                yield None
                continue

            kind, payload = item
            if kind == "done":
                break

            if kind == "error":
                if isinstance(payload, Exception):
                    code, retryable, message = map_provider_error(payload)
                else:
                    code, retryable, message = (
                        "INTERNAL_ERROR",
                        False,
                        "服务内部错误",
                    )
                yield SSEEvent(
                    "error",
                    {
                        "code": code,
                        "message": message,
                        "retryable": retryable,
                        "run_id": run_id,
                    },
                )
                return

            event = payload
            if not isinstance(event, StreamEvent):
                continue

            if event.type == "text_delta":
                yield SSEEvent("content_delta", {"delta": event.delta})
            elif event.type == "reasoning_delta":
                yield SSEEvent("reasoning_delta", {"delta": event.delta})
            elif event.type == "usage":
                usage = event.usage
                yield SSEEvent("usage", _usage_dict(usage) or {})
            elif event.type == "end":
                finish_reason = event.finish_reason or "stop"

        yield SSEEvent(
            "message_end",
            {
                "finish_reason": finish_reason,
                "message_id": message_id,
                "run_id": run_id,
                "usage": _usage_dict(usage),
            },
        )
    finally:
        if not task.done():
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
