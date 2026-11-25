import pytest

from tests.helpers import stream_chat, text_of
from tests.providers import FailingProvider, RecordingProvider


@pytest.mark.asyncio
async def test_multi_turn_context_includes_history(client, app, settings):
    provider = RecordingProvider("收到。")
    app.state.provider = provider

    first = await stream_chat(
        client,
        settings,
        "我叫小明",
        client_message_id="c1",
    )
    conversation_id = first[0]["data"]["conversation_id"]

    second = await stream_chat(
        client,
        settings,
        "我叫什么",
        conversation_id=conversation_id,
        client_message_id="c2",
    )

    assert len(provider.calls) == 2

    first_messages = provider.calls[0]
    second_messages = provider.calls[1]

    assert [message.role for message in first_messages] == ["system", "user"]
    assert first_messages[-1].content == "我叫小明"

    assert [message.role for message in second_messages] == [
        "system",
        "user",
        "assistant",
        "user",
    ]
    assert second_messages[1].content == "我叫小明"
    assert second_messages[2].content == "收到。"
    assert second_messages[3].content == "我叫什么"

    assert text_of(first) == "收到。"
    assert text_of(second) == "收到。"


@pytest.mark.asyncio
async def test_idempotent_replay_does_not_call_provider_twice(client, app, settings):
    provider = RecordingProvider("收到。")
    app.state.provider = provider

    first = await stream_chat(
        client,
        settings,
        "你好",
        client_message_id="same-key",
    )
    conversation_id = first[0]["data"]["conversation_id"]

    second = await stream_chat(
        client,
        settings,
        "你好",
        conversation_id=conversation_id,
        client_message_id="same-key",
    )

    assert len(provider.calls) == 1
    assert text_of(second) == "收到。"
    assert second[-1]["event"] == "message_end"
    assert second[-1]["data"]["replayed"] is True


@pytest.mark.asyncio
async def test_history_endpoint_returns_persisted_messages(client, app, settings):
    app.state.provider = RecordingProvider("收到。")

    events = await stream_chat(
        client,
        settings,
        "你好",
        client_message_id="hist-1",
    )
    conversation_id = events[0]["data"]["conversation_id"]

    response = await client.get(
        f"/v1/conversations/{conversation_id}/messages",
        headers={"Authorization": f"Bearer {settings.dev_token}"},
    )

    assert response.status_code == 200
    messages = response.json()["messages"]
    assert [message["role"] for message in messages] == ["user", "assistant"]
    assert messages[0]["content"] == "你好"
    assert messages[1]["content"] == "收到。"
    assert messages[1]["status"] == "completed"


@pytest.mark.asyncio
async def test_conversations_list_and_title(client, app, settings):
    app.state.provider = RecordingProvider("收到。")

    await stream_chat(
        client,
        settings,
        "我叫小明",
        client_message_id="list-1",
    )

    response = await client.get(
        "/v1/conversations",
        headers={"Authorization": f"Bearer {settings.dev_token}"},
    )

    assert response.status_code == 200
    conversations = response.json()["conversations"]
    assert len(conversations) == 1
    assert conversations[0]["title"] == "我叫小明"


@pytest.mark.asyncio
async def test_unknown_conversation_returns_404(client, settings):
    headers = {"Authorization": f"Bearer {settings.dev_token}"}

    response = await client.get(
        "/v1/conversations/c_missing/messages",
        headers=headers,
    )
    assert response.status_code == 404

    response = await client.post(
        "/v1/chat/stream",
        headers=headers,
        json={"message": "你好", "conversation_id": "c_missing"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_failed_stream_persists_partial_message(client, app, settings):
    app.state.provider = FailingProvider("半句")

    events = await stream_chat(
        client,
        settings,
        "你好",
        client_message_id="fail-1",
    )
    conversation_id = events[0]["data"]["conversation_id"]
    assert any(event["event"] == "error" for event in events)

    response = await client.get(
        f"/v1/conversations/{conversation_id}/messages",
        headers={"Authorization": f"Bearer {settings.dev_token}"},
    )

    assert response.status_code == 200
    messages = response.json()["messages"]
    assert [message["role"] for message in messages] == ["user", "assistant"]
    assert messages[1]["content"] == "半句"
    assert messages[1]["status"] == "failed"
