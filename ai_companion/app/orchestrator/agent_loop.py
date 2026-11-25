import logging
from collections.abc import AsyncIterator

from app.config import Settings
from app.llm.base import LLMProvider, Message, ToolCall, ToolSpec, Usage
from app.llm.errors import map_provider_error
from app.orchestrator.events import SSEEvent
from app.orchestrator.stream_utils import with_heartbeat
from app.orchestrator.token_counter import estimate_messages, estimate_tool_specs
from app.tools.recorder import ToolCallRecorder
from app.tools.registry import ToolRegistry

logger = logging.getLogger(__name__)


def _usage_dict(usage: Usage | None, *, step: int | None = None) -> dict[str, object]:
    payload: dict[str, object] = {
        "input_tokens": usage.input_tokens if usage is not None else 0,
        "output_tokens": usage.output_tokens if usage is not None else 0,
        "cached_tokens": usage.cached_tokens if usage is not None else 0,
    }
    if step is not None:
        payload["step"] = step
    return payload


def _add_usage(total: Usage, delta: Usage | None) -> None:
    if delta is None:
        return
    total.input_tokens += delta.input_tokens
    total.output_tokens += delta.output_tokens
    total.cached_tokens += delta.cached_tokens


async def stream_turn(
    provider: LLMProvider,
    messages: list[Message],
    tools: list[ToolSpec],
    registry: ToolRegistry,
    recorder: ToolCallRecorder,
    *,
    conversation_id: str,
    message_id: str,
    run_id: str,
    created_at: str,
    settings: Settings,
    allowed_tools: set[str] | None = None,
) -> AsyncIterator[SSEEvent | None]:
    """Run the model/tool/model loop and yield protocol events.

    The loop is intentionally hand-written: each iteration is one model call.
    If the model asks for tools, we execute them, append the tool results, and
    call the model again. The client sees one continuous SSE stream.
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

    total_usage = Usage()
    tool_spec_tokens = estimate_tool_specs(tools)
    max_input_tokens = settings.context_window_tokens - settings.llm_max_tokens

    for step in range(1, settings.max_tool_steps + 1):
        if estimate_messages(messages) + tool_spec_tokens > max_input_tokens:
            yield SSEEvent(
                "error",
                {
                    "code": "TOOL_CONTEXT_OVERFLOW",
                    "message": "工具上下文超出预算",
                    "retryable": False,
                    "run_id": run_id,
                    "step": step,
                },
            )
            return

        step_text: list[str] = []
        step_tool_calls: list[ToolCall] = []
        step_finish_reason = "stop"

        try:
            async for item in with_heartbeat(
                provider.stream(messages, tools),
                settings.sse_heartbeat_seconds,
                run_id=run_id,
            ):
                if item is None:
                    yield None
                    continue

                if item.type == "text_delta":
                    step_text.append(item.delta)
                    yield SSEEvent(
                        "content_delta",
                        {"delta": item.delta, "step": step},
                    )
                elif item.type == "reasoning_delta":
                    yield SSEEvent(
                        "reasoning_delta",
                        {"delta": item.delta, "step": step},
                    )
                elif item.type == "tool_calls":
                    step_tool_calls = item.tool_calls or []
                elif item.type == "usage":
                    _add_usage(total_usage, item.usage)
                    yield SSEEvent(
                        "usage",
                        _usage_dict(item.usage, step=step),
                    )
                elif item.type == "end":
                    step_finish_reason = item.finish_reason or "stop"
        except Exception as exc:  # noqa: BLE001 - boundary converts to SSE error
            code, retryable, message = map_provider_error(exc)
            logger.exception(
                "agent provider stream failed run_id=%s step=%s",
                run_id,
                step,
            )
            yield SSEEvent(
                "error",
                {
                    "code": code,
                    "message": message,
                    "retryable": retryable,
                    "run_id": run_id,
                    "step": step,
                },
            )
            return

        if not step_tool_calls:
            yield SSEEvent(
                "message_end",
                {
                    "finish_reason": step_finish_reason,
                    "message_id": message_id,
                    "run_id": run_id,
                    "usage": _usage_dict(total_usage),
                },
            )
            return

        for call in step_tool_calls:
            yield SSEEvent(
                "tool_call",
                {
                    "run_id": run_id,
                    "step": step,
                    "call_id": call.id,
                    "name": call.name,
                    "server": registry.server_for(call.name),
                    "arguments": call.arguments,
                },
            )

        results = await registry.execute_many(
            step_tool_calls,
            allowed=allowed_tools,
        )

        for call, result in zip(step_tool_calls, results):
            await recorder.record(
                run_id=run_id,
                step=step,
                call=call,
                result=result,
            )
            yield SSEEvent(
                "tool_result",
                {
                    "run_id": run_id,
                    "step": step,
                    "call_id": result.call_id,
                    "name": result.name,
                    "status": result.status,
                    "preview": result.content[:200],
                    "latency_ms": result.latency_ms,
                    "error_code": result.error_code,
                },
            )

        messages.append(
            Message(
                role="assistant",
                content="".join(step_text),
                tool_calls=step_tool_calls,
            )
        )
        for result in results:
            messages.append(
                Message(
                    role="tool",
                    content=result.content,
                    tool_call_id=result.call_id,
                    name=result.name,
                )
            )

    yield SSEEvent(
        "error",
        {
            "code": "TOOL_LOOP_LIMIT",
            "message": "工具调用次数超限",
            "retryable": False,
            "run_id": run_id,
        },
    )
