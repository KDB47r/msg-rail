import pytest

from tests.helpers import parse_sse


@pytest.mark.asyncio
async def test_chat_stream_returns_expected_events(client, settings):
    async with client.stream(
        "POST",
        "/v1/chat/stream",
        headers={"Authorization": f"Bearer {settings.dev_token}"},
        json={"message": "你好"},
    ) as response:
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/event-stream")

        body = ""
        async for chunk in response.aiter_text():
            body += chunk

    events = parse_sse(body)

    assert events[0]["event"] == "message_start"
    assert events[0]["data"]["conversation_id"].startswith("c_")
    assert events[0]["data"]["message_id"].startswith("m_")
    assert events[0]["data"]["run_id"].startswith("r_")
    assert events[-1]["event"] == "message_end"

    text = "".join(
        event["data"]["delta"]
        for event in events
        if event["event"] == "content_delta"
    )
    assert text == settings.fake_response

    assert any(event["event"] == "usage" for event in events)
    assert all(event["id"] is not None for event in events)


@pytest.mark.asyncio
async def test_chat_stream_requires_auth(client):
    response = await client.post("/v1/chat/stream", json={"message": "你好"})

    assert response.status_code == 401


@pytest.mark.asyncio
async def test_chat_stream_rejects_empty_message(client, settings):
    response = await client.post(
        "/v1/chat/stream",
        headers={"Authorization": f"Bearer {settings.dev_token}"},
        json={"message": ""},
    )

    assert response.status_code == 422
