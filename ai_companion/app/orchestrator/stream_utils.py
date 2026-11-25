import asyncio
import contextlib
from collections.abc import AsyncIterator

from app.llm.base import StreamEvent


async def with_heartbeat(
    events: AsyncIterator[StreamEvent],
    interval: float,
    *,
    run_id: str,
) -> AsyncIterator[StreamEvent | None]:
    """Yield provider events, inserting ``None`` heartbeats while waiting."""

    queue: asyncio.Queue[tuple[str, object]] = asyncio.Queue(maxsize=100)

    async def producer() -> None:
        cancelled = False
        try:
            async for event in events:
                await queue.put(("event", event))
        except asyncio.CancelledError:
            cancelled = True
            raise
        except Exception as exc:  # noqa: BLE001 - boundary hands errors to consumer
            await queue.put(("error", exc))
        finally:
            if not cancelled:
                await queue.put(("done", None))

    task = asyncio.create_task(producer(), name=f"llm-{run_id}")

    try:
        while True:
            try:
                if interval > 0:
                    item = await asyncio.wait_for(
                        queue.get(),
                        timeout=interval,
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
                    raise payload
                raise RuntimeError("provider stream failed")

            if isinstance(payload, StreamEvent):
                yield payload
    finally:
        if not task.done():
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
