import pytest

from app.llm.base import ToolCall, ToolSpec
from tests.helpers import parse_sse, text_of
from tests.providers import RecordingProvider, ToolCallingProvider


def _payload(**overrides):
    payload = {
        "request_id": "r_test",
        "mode": "shared",
        "session_id": "single:42:43",
        "bot_user_id": 10002,
        "trigger_message_id": "single:42:43-4",
        "trigger_sender_id": 42,
        "trigger_sender_name": "张三",
        "context_messages": [
            {
                "message_id": "single:42:43-1",
                "sender_id": 42,
                "sender_name": "张三",
                "sender_type": "human",
                "content": "今天好累",
            },
            {
                "message_id": "single:42:43-2",
                "sender_id": 43,
                "sender_name": "李四",
                "sender_type": "human",
                "content": "那就早点休息",
            },
            {
                "message_id": "single:42:43-3",
                "sender_id": 43,
                "sender_name": "李四",
                "sender_type": "bot",
                "content": "两位别太卷啦。",
            },
            {
                "message_id": "single:42:43-4",
                "sender_id": 42,
                "sender_name": "张三",
                "sender_type": "human",
                "content": "@小桁 你觉得呢",
            },
        ],
    }
    payload.update(overrides)
    return payload


async def _stream(client, settings, payload):
    async with client.stream(
        "POST",
        "/internal/v1/agent/stream",
        headers={"Authorization": f"Bearer {settings.internal_token}"},
        json=payload,
    ) as response:
        assert response.status_code == 200
        body = ""
        async for chunk in response.aiter_text():
            body += chunk
    return parse_sse(body)


@pytest.mark.asyncio
async def test_internal_requires_auth(client):
    response = await client.post(
        "/internal/v1/agent/stream",
        json=_payload(),
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_internal_shared_stream_and_prompt(monkeypatch, app, client, settings):
    provider = RecordingProvider(text="别卷了。")
    app.state.provider = provider

    events = await _stream(client, settings, _payload())

    assert events[0]["event"] == "message_start"
    assert events[-1]["event"] == "message_end"
    assert text_of(events) == "别卷了。"

    assert provider.calls
    messages = provider.calls[0]
    assert messages[0].role == "system"
    assert "气氛组" in messages[0].content
    assert messages[-1].role == "user"
    assert "张三：@小桁 你觉得呢" in messages[-1].content
    assert "李四：那就早点休息" in messages[-1].content
    assert "两位别太卷啦。" in messages[-1].content


@pytest.mark.asyncio
async def test_internal_shared_ignores_messages_after_trigger(
    app,
    client,
    settings,
):
    provider = RecordingProvider(text="收到。")
    app.state.provider = provider
    payload = _payload()
    payload["context_messages"].append(
        {
            "message_id": "single:42:43-5",
            "sender_id": 43,
            "sender_name": "李四",
            "sender_type": "human",
            "content": "这条在触发消息之后，不应该进入上下文",
        }
    )

    await _stream(client, settings, payload)

    prompt = provider.calls[0][-1].content
    assert "这条在触发消息之后" not in prompt


@pytest.mark.asyncio
async def test_internal_private_uses_user_and_assistant_roles(
    app,
    client,
    settings,
):
    provider = RecordingProvider(text="我在。")
    app.state.provider = provider
    payload = _payload(
        mode="private",
        session_id="single:42:10001",
        bot_user_id=10001,
        trigger_message_id="single:42:10001-3",
        trigger_sender_name="张三",
        context_messages=[
            {
                "message_id": "single:42:10001-1",
                "sender_id": 42,
                "sender_name": "张三",
                "sender_type": "human",
                "content": "你好",
            },
            {
                "message_id": "single:42:10001-2",
                "sender_id": 10001,
                "sender_name": "小桁",
                "sender_type": "bot",
                "content": "你好呀",
            },
            {
                "message_id": "single:42:10001-3",
                "sender_id": 42,
                "sender_name": "张三",
                "sender_type": "human",
                "content": "今天吃什么",
            },
        ],
    )

    events = await _stream(client, settings, payload)

    assert text_of(events) == "我在。"
    roles = [message.role for message in provider.calls[0]]
    assert roles == ["system", "user", "assistant", "system", "user"]
    assert "私人 AI 助手" in provider.calls[0][0].content
    assert "本轮校准" in provider.calls[0][-2].content


@pytest.mark.asyncio
async def test_internal_shared_tool_profile_denies_private_tool(
    app,
    client,
    settings,
):
    settings.tools_enabled = True
    settings.shared_tool_allowlist = "fun__calculator"

    async def fetch_executor(arguments):
        return "should not run"

    async def calc_executor(arguments):
        return "1+1=2"

    app.state.tool_registry.register(
        ToolSpec(
            name="fun__calculator",
            description="calculator",
            parameters={"type": "object"},
        ),
        calc_executor,
        server="fun",
        permission="read",
    )
    app.state.tool_registry.register(
        ToolSpec(
            name="web__fetch",
            description="fetch",
            parameters={"type": "object"},
        ),
        fetch_executor,
        server="web",
        permission="read",
    )
    call = ToolCall(
        id="call_denied",
        name="web__fetch",
        arguments={"url": "https://example.com"},
    )
    app.state.provider = ToolCallingProvider([call], "不查了。")

    events = await _stream(client, settings, _payload())
    result = next(
        event for event in events if event["event"] == "tool_result"
    )
    assert result["data"]["status"] == "denied"
    assert result["data"]["error_code"] == "TOOL_DENIED"


@pytest.mark.asyncio
async def test_internal_shared_tool_profile_allows_fun_tool(
    app,
    client,
    settings,
):
    settings.tools_enabled = True
    settings.shared_tool_allowlist = "fun__calculator"

    async def calc_executor(arguments):
        return f"计算结果：{arguments.get('expression')}"

    app.state.tool_registry.register(
        ToolSpec(
            name="fun__calculator",
            description="calculator",
            parameters={"type": "object"},
        ),
        calc_executor,
        server="fun",
        permission="read",
    )
    call = ToolCall(
        id="call_calc",
        name="fun__calculator",
        arguments={"expression": "1+1"},
    )
    app.state.provider = ToolCallingProvider([call], "等于 2。")

    events = await _stream(client, settings, _payload())
    result = next(
        event for event in events if event["event"] == "tool_result"
    )
    assert result["data"]["status"] == "success"


@pytest.mark.asyncio
async def test_internal_rejects_empty_context(client, settings):
    payload = _payload(context_messages=[])
    response = await client.post(
        "/internal/v1/agent/stream",
        headers={"Authorization": f"Bearer {settings.internal_token}"},
        json=payload,
    )
    assert response.status_code == 422
