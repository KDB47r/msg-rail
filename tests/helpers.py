import json

from httpx import AsyncClient

from app.config import Settings


def parse_sse(text: str) -> list[dict]:
    """Parse the subset of SSE used by this service into dicts."""

    events: list[dict] = []
    normalized = text.replace("\r\n", "\n").strip()
    if not normalized:
        return events

    for block in normalized.split("\n\n"):
        if not block.strip():
            continue

        event = None
        event_id = None
        data_lines: list[str] = []

        for line in block.split("\n"):
            if line.startswith(":"):
                continue
            if line.startswith("event:"):
                event = line[len("event:") :].strip()
            elif line.startswith("data:"):
                data_lines.append(line[len("data:") :].strip())
            elif line.startswith("id:"):
                event_id = int(line[len("id:") :].strip())

        if event is None:
            continue

        events.append(
            {
                "event": event,
                "data": json.loads("\n".join(data_lines)) if data_lines else None,
                "id": event_id,
            }
        )

    return events


def text_of(events: list[dict]) -> str:
    return "".join(
        event["data"]["delta"]
        for event in events
        if event["event"] == "content_delta"
    )


async def stream_chat(
    client: AsyncClient,
    settings: Settings,
    message: str,
    *,
    conversation_id: str | None = None,
    client_message_id: str | None = None,
) -> list[dict]:
    payload: dict[str, object] = {"message": message}
    if conversation_id is not None:
        payload["conversation_id"] = conversation_id
    if client_message_id is not None:
        payload["client_message_id"] = client_message_id

    headers = {"Authorization": f"Bearer {settings.dev_token}"}
    async with client.stream(
        "POST",
        "/v1/chat/stream",
        headers=headers,
        json=payload,
    ) as response:
        assert response.status_code == 200
        body = ""
        async for chunk in response.aiter_text():
            body += chunk

    return parse_sse(body)
