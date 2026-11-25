import pytest
from sqlalchemy import select

from app.llm.base import ToolCall, ToolSpec
from app.storage.models import ToolCallRecord
from app.tools.base import LocalTool
from tests.helpers import stream_chat
from tests.providers import AlwaysToolProvider, ToolCallingProvider


async def _get_weather(city: str) -> str:
    return f"{city}：晴，26℃"


@pytest.mark.asyncio
async def test_agent_loop_executes_tool_and_audits(client, app, settings):
    settings.tools_enabled = True
    settings.max_tool_steps = 3

    spec = ToolSpec(
        name="test__get_weather",
        description="查询天气",
        parameters={
            "type": "object",
            "properties": {"city": {"type": "string"}},
            "required": ["city"],
        },
    )
    app.state.tool_registry.register(
        spec,
        LocalTool(spec, _get_weather).execute,
        server="test",
        permission="read",
    )

    call = ToolCall(
        id="call_1",
        name="test__get_weather",
        arguments={"city": "杭州"},
    )
    provider = ToolCallingProvider([call], "杭州今天晴，26℃。")
    app.state.provider = provider

    events = await stream_chat(
        client,
        settings,
        "杭州天气怎么样？",
        client_message_id="p3-1",
    )
    names = [event["event"] for event in events]

    assert names[0] == "message_start"
    assert "tool_call" in names
    assert "tool_result" in names
    assert "content_delta" in names
    assert names[-1] == "message_end"

    tool_call = next(event for event in events if event["event"] == "tool_call")
    assert tool_call["data"]["name"] == "test__get_weather"
    assert tool_call["data"]["arguments"] == {"city": "杭州"}

    tool_result = next(event for event in events if event["event"] == "tool_result")
    assert tool_result["data"]["status"] == "success"
    assert "26" in tool_result["data"]["preview"]

    assert len(provider.received_messages) == 2
    second_messages = provider.received_messages[1]
    assert any(
        message.role == "tool" and "26" in message.content
        for message in second_messages
    )
    assert any(
        message.role == "assistant" and message.tool_calls
        for message in second_messages
    )

    async with app.state.session_factory() as session:
        rows = (await session.scalars(select(ToolCallRecord))).all()

    assert len(rows) == 1
    assert rows[0].tool_name == "test__get_weather"
    assert rows[0].status == "success"


@pytest.mark.asyncio
async def test_tool_error_is_returned_to_model(client, app, settings):
    settings.tools_enabled = True

    async def broken(city: str) -> str:
        raise RuntimeError("weather down")

    spec = ToolSpec(
        name="test__broken",
        description="broken",
        parameters={"type": "object"},
    )
    app.state.tool_registry.register(
        spec,
        LocalTool(spec, broken).execute,
        server="test",
        permission="read",
    )

    call = ToolCall(id="call_1", name="test__broken", arguments={})
    app.state.provider = ToolCallingProvider([call], "天气服务暂时不可用。")

    events = await stream_chat(
        client,
        settings,
        "天气",
        client_message_id="p3-err",
    )
    tool_result = next(event for event in events if event["event"] == "tool_result")
    assert tool_result["data"]["status"] == "error"

    async with app.state.session_factory() as session:
        rows = (await session.scalars(select(ToolCallRecord))).all()

    assert len(rows) == 1
    assert rows[0].status == "error"


@pytest.mark.asyncio
async def test_max_tool_steps_returns_error(client, app, settings):
    settings.tools_enabled = True
    settings.max_tool_steps = 2

    async def noop(arguments: dict[str, object]) -> str:
        return "ok"

    spec = ToolSpec(
        name="test__noop",
        description="noop",
        parameters={"type": "object"},
    )
    app.state.tool_registry.register(
        spec,
        noop,
        server="test",
        permission="read",
    )
    app.state.provider = AlwaysToolProvider("test__noop", {})

    events = await stream_chat(
        client,
        settings,
        "循环",
        client_message_id="p3-limit",
    )
    errors = [event for event in events if event["event"] == "error"]

    assert errors
    assert errors[-1]["data"]["code"] == "TOOL_LOOP_LIMIT"
