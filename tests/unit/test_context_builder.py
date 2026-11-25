import pytest

from app.llm.base import Message
from app.orchestrator.context_builder import (
    ContextTooLarge,
    build_context,
    build_messages,
    build_p0_messages,
)


def test_build_p0_messages_has_system_then_user():
    messages = build_p0_messages("你好")

    assert [message.role for message in messages] == ["system", "user"]
    assert messages[-1].content == "你好"
    assert messages[0].content


def test_build_messages_injects_history_between_system_and_user():
    history = [
        Message(role="user", content="我叫小明"),
        Message(role="assistant", content="你好，小明"),
    ]

    messages = build_messages(history, "我叫什么")

    assert [message.role for message in messages] == [
        "system",
        "user",
        "assistant",
        "user",
    ]
    assert messages[1].content == "我叫小明"
    assert messages[2].content == "你好，小明"
    assert messages[-1].content == "我叫什么"


def test_build_messages_drops_empty_and_non_dialog_roles():
    history = [
        Message(role="system", content="应该被忽略"),
        Message(role="user", content=""),
        Message(role="assistant", content="保留"),
    ]

    messages = build_messages(history, "hi")

    assert [message.role for message in messages] == ["system", "assistant", "user"]
    assert messages[1].content == "保留"


def test_build_context_drops_oldest_when_budget_is_small():
    history = [
        Message(role="user", content="第一轮" * 20),
        Message(role="assistant", content="第一轮回复" * 20),
        Message(role="user", content="第二轮"),
        Message(role="assistant", content="第二轮回复"),
    ]

    result = build_context(
        history,
        None,
        "当前问题",
        context_window_tokens=300,
        output_reserve_tokens=250,
        safety_margin=1.0,
        summary_trigger_ratio=0.5,
        system_prompt="系统",
    )

    assert result.dropped_messages > 0
    assert result.needs_summary is True
    assert result.messages[-1].content == "当前问题"
    assert all("第一轮" not in message.content for message in result.messages)
    assert any("第二轮" in message.content for message in result.messages)


def test_build_context_injects_summary_into_system():
    result = build_context(
        [Message(role="user", content="最近一句")],
        "用户叫小明。",
        "你好",
        context_window_tokens=1000,
        output_reserve_tokens=100,
        safety_margin=1.0,
        summary_trigger_ratio=0.9,
    )

    assert "用户叫小明" in result.messages[0].content
    assert result.messages[-1].content == "你好"


def test_trigger_uses_available_history_budget_not_full_window():
    history = [Message(role="user", content="你好" * 60)]

    result = build_context(
        history,
        None,
        "你好",
        context_window_tokens=1000,
        output_reserve_tokens=840,
        safety_margin=1.0,
        summary_trigger_ratio=0.7,
        system_prompt="系统",
    )

    # The available history budget still fits the 124-token history, while
    # 70% of the budget is below it, so trigger strategy B fires.
    assert result.dropped_messages == 0
    assert result.needs_summary is True


def test_build_context_raises_when_current_message_is_too_large():
    with pytest.raises(ContextTooLarge):
        build_context(
            [],
            None,
            "很长的消息" * 500,
            context_window_tokens=100,
            output_reserve_tokens=50,
            safety_margin=1.0,
            summary_trigger_ratio=0.9,
        )
