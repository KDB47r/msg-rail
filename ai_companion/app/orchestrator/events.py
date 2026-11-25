import json
from dataclasses import dataclass


@dataclass(slots=True)
class SSEEvent:
    event: str
    data: dict[str, object]


def format_sse(event: str, data: dict[str, object], event_id: int) -> str:
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    return f"id: {event_id}\nevent: {event}\ndata: {payload}\n\n"


def heartbeat() -> str:
    return ": ping\n\n"
