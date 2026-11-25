import asyncio
from collections.abc import AsyncIterator

from app.llm.base import Message, StreamEvent, ToolSpec, Usage


def _estimate_tokens(text: str) -> int:
    return max(1, len(text) // 4)


class FakeProvider:
    """Deterministic provider for local development and tests."""

    def __init__(self, text: str, delay: float = 0.02, chunk_size: int = 4) -> None:
        self._text = text
        self._delay = max(0.0, delay)
        self._chunk_size = max(1, chunk_size)

    async def stream(
        self,
        messages: list[Message],
        tools: list[ToolSpec] | None = None,
    ) -> AsyncIterator[StreamEvent]:
        for start in range(0, len(self._text), self._chunk_size):
            if self._delay:
                await asyncio.sleep(self._delay)
            yield StreamEvent(
                type="text_delta",
                delta=self._text[start : start + self._chunk_size],
            )

        prompt_text = "".join(message.content for message in messages)
        yield StreamEvent(
            type="usage",
            usage=Usage(
                input_tokens=_estimate_tokens(prompt_text),
                output_tokens=_estimate_tokens(self._text),
            ),
        )
        yield StreamEvent(type="end", finish_reason="stop")
