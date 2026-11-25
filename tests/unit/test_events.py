from app.orchestrator.events import format_sse, heartbeat


def test_format_sse_includes_id_event_and_json():
    raw = format_sse("content_delta", {"delta": "你好"}, 3)

    assert raw.startswith("id: 3\nevent: content_delta\ndata: ")
    assert raw.endswith("\n\n")
    assert '"你好"' in raw


def test_heartbeat_is_sse_comment():
    assert heartbeat() == ": ping\n\n"
