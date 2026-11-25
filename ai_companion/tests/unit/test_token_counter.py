from app.llm.base import Message
from app.orchestrator.token_counter import (
    estimate_message,
    estimate_messages,
    estimate_text,
)


def test_estimate_text_chinese_and_ascii():
    assert estimate_text("") == 0
    assert estimate_text("你好") == 2
    assert estimate_text("hello") == 2
    assert estimate_text("你好hello") == 4


def test_estimate_message_adds_overhead():
    message = Message(role="user", content="你好")

    assert estimate_message(message) == estimate_text("你好") + 4


def test_estimate_messages_sums_messages():
    messages = [
        Message(role="user", content="你好"),
        Message(role="assistant", content="hello"),
    ]

    assert estimate_messages(messages) == sum(
        estimate_message(message) for message in messages
    )
