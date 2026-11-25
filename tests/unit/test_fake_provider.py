import pytest

from app.llm.base import Message
from app.llm.fake import FakeProvider


@pytest.mark.asyncio
async def test_fake_provider_streams_full_text_and_end():
    provider = FakeProvider(text="你好呀", delay=0.0, chunk_size=1)

    events = [
        event
        async for event in provider.stream([Message(role="user", content="hi")])
    ]

    text = "".join(event.delta for event in events if event.type == "text_delta")
    assert text == "你好呀"
    assert events[-1].type == "end"
    assert events[-1].finish_reason == "stop"
    assert any(event.type == "usage" for event in events)
