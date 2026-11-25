from types import SimpleNamespace

from app.llm.base import Message, ToolCall
from app.llm.openai_compat import _ToolCallAccumulator, _message_payload


def test_accumulator_joins_streamed_fragments():
    accumulator = _ToolCallAccumulator()

    accumulator.add(
        SimpleNamespace(
            index=0,
            id="call_1",
            function=SimpleNamespace(name="get_weather", arguments='{"city":'),
        )
    )
    accumulator.add(
        SimpleNamespace(
            index=0,
            id=None,
            function=SimpleNamespace(name=None, arguments='"杭州"}'),
        )
    )

    calls = accumulator.build()

    assert len(calls) == 1
    assert calls[0].id == "call_1"
    assert calls[0].name == "get_weather"
    assert calls[0].arguments == {"city": "杭州"}
    assert calls[0].parse_error is False


def test_accumulator_marks_invalid_json():
    accumulator = _ToolCallAccumulator()
    accumulator.add(
        SimpleNamespace(
            index=0,
            id="call_1",
            function=SimpleNamespace(name="get_weather", arguments="{"),
        )
    )

    calls = accumulator.build()

    assert calls[0].parse_error is True
    assert calls[0].arguments == {}


def test_message_payload_maps_tool_messages():
    assistant = Message(
        role="assistant",
        content="",
        tool_calls=[
            ToolCall(
                id="call_1",
                name="get_weather",
                arguments={"city": "杭州"},
            )
        ],
    )
    tool = Message(
        role="tool",
        content="26℃",
        tool_call_id="call_1",
        name="get_weather",
    )

    assistant_payload = _message_payload(assistant)
    tool_payload = _message_payload(tool)

    assert assistant_payload["tool_calls"][0]["function"]["name"] == "get_weather"
    assert assistant_payload["tool_calls"][0]["id"] == "call_1"
    assert tool_payload["tool_call_id"] == "call_1"
    assert tool_payload["content"] == "26℃"
