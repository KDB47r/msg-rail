import asyncio
from collections.abc import AsyncIterator

from app.llm.base import Message, StreamEvent, ToolCall, ToolSpec, Usage


class RecordingProvider:
    """Records the messages passed to the model, then streams a fixed reply."""

    def __init__(self, text: str = "收到。", delay: float = 0.0) -> None:
        self.text = text
        self.delay = delay
        self.calls: list[list[Message]] = []

    async def stream(
        self,
        messages: list[Message],
        tools: list[ToolSpec] | None = None,
    ) -> AsyncIterator[StreamEvent]:
        self.calls.append(list(messages))
        for char in self.text:
            if self.delay:
                await asyncio.sleep(self.delay)
            yield StreamEvent(type="text_delta", delta=char)

        yield StreamEvent(
            type="usage",
            usage=Usage(input_tokens=10, output_tokens=5),
        )
        yield StreamEvent(type="end", finish_reason="stop")


class ToolCallingProvider:
    """First call requests tools, second call returns the final answer."""

    def __init__(
        self,
        tool_calls: list[ToolCall],
        final_text: str = "完成。",
    ) -> None:
        self.tool_calls = tool_calls
        self.final_text = final_text
        self.calls = 0
        self.received_messages: list[list[Message]] = []

    async def stream(
        self,
        messages: list[Message],
        tools: list[ToolSpec] | None = None,
    ) -> AsyncIterator[StreamEvent]:
        self.calls += 1
        self.received_messages.append(list(messages))

        if self.calls == 1:
            yield StreamEvent(type="tool_calls", tool_calls=list(self.tool_calls))
            yield StreamEvent(
                type="usage",
                usage=Usage(input_tokens=10, output_tokens=5),
            )
            yield StreamEvent(type="end", finish_reason="tool_calls")
            return

        for char in self.final_text:
            yield StreamEvent(type="text_delta", delta=char)
        yield StreamEvent(
            type="usage",
            usage=Usage(input_tokens=20, output_tokens=5),
        )
        yield StreamEvent(type="end", finish_reason="stop")


class AlwaysToolProvider:
    """Always requests a tool, used to test the max-step guard."""

    def __init__(self, name: str, arguments: dict[str, object]) -> None:
        self.name = name
        self.arguments = arguments
        self.calls = 0

    async def stream(
        self,
        messages: list[Message],
        tools: list[ToolSpec] | None = None,
    ) -> AsyncIterator[StreamEvent]:
        self.calls += 1
        call = ToolCall(
            id=f"call_{self.calls}",
            name=self.name,
            arguments=dict(self.arguments),
        )
        yield StreamEvent(type="tool_calls", tool_calls=[call])
        yield StreamEvent(type="end", finish_reason="tool_calls")


class FailingProvider:
    """Yields one partial delta, then fails, to test failure persistence."""

    def __init__(self, partial: str = "半句") -> None:
        self.partial = partial

    async def stream(
        self,
        messages: list[Message],
        tools: list[ToolSpec] | None = None,
    ) -> AsyncIterator[StreamEvent]:
        if self.partial:
            yield StreamEvent(type="text_delta", delta=self.partial)
        raise RuntimeError("provider boom")
