"""Build model messages for MsgRail's internal AI endpoint.

The IM service owns the chat history. This module only turns the context it
sends into a prompt; it never reads or writes AI Companion conversations.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from math import ceil
from pathlib import Path
from typing import Literal

from app.llm.base import Message
from app.orchestrator.prompt_context import current_date_line
from app.orchestrator.token_counter import estimate_text

PROMPT_DIR = Path(__file__).resolve().parent.parent / "prompts"
PERSONA_FILES = {
    "private": PROMPT_DIR / "xiaoheng_private.md",
    "shared": PROMPT_DIR / "xiaoheng_shared.md",
}
FALLBACK_PROMPTS = {
    "private": "你是小桁，用户的私人 AI 助手。用中文简短回复，不要假装是人类。",
    "shared": "你是小桁，A、B 单聊里的气氛组。只有被 @ 时简短回复，不要假装是人类。",
}

MAX_MESSAGE_CHARS = 2000
MAX_CONTEXT_MESSAGES = 30
_STALE_DATE_RE = re.compile(
    r"2024年|训练数据截止|训练截止|知识截止|knowledge cutoff",
    re.IGNORECASE,
)


def _calibration_text() -> str:
    return (
        "【本轮校准】"
        + current_date_line()
        + " 历史消息里出现的日期、训练截止说明、时事结论、人物近况，以及"
        "“我已经调用过工具”的说法都可能是过期或未实际发生的，不要沿用；"
        "用户再次追问时必须重新查。系统已提供当前日期和时间，可以直接使用，"
        "但不要声称调用了工具；只有本轮真的产生工具调用，才能说工具结果。"
        "涉及新闻、人物近况等实时信息时，必须在本轮真正调用可用工具；只有"
        "本轮工具返回的结果才能当作工具结果。"
    )


def _sanitize_stale_bot_history(messages: list[IMContextMessage]) -> None:
    """Drop stale cutoff/date claims from old assistant turns.

    Old assistant text can poison a new turn: the model may copy “知识截止
    2024年” or pretend it called a tool. The database/history is untouched;
    only the prompt view is replaced.
    """

    for message in messages:
        if message.sender_type == "bot" and _STALE_DATE_RE.search(message.content):
            message.content = "[旧回答中涉及日期/训练截止的内容已过期，已省略]"


@dataclass(slots=True)
class IMContextMessage:
    message_id: str
    sender_id: int
    sender_name: str
    sender_type: Literal["human", "bot"]
    content: str
    timestamp_ms: int = 0


def load_persona(mode: Literal["private", "shared"]) -> str:
    path = PERSONA_FILES[mode]
    try:
        return path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return FALLBACK_PROMPTS[mode]


def _trim_content(content: str) -> str:
    content = content.strip()
    if len(content) <= MAX_MESSAGE_CHARS:
        return content
    return content[:MAX_MESSAGE_CHARS] + "……"


def _select_context(
    messages: list[IMContextMessage],
    *,
    max_tokens: int,
) -> list[IMContextMessage]:
    """Keep the newest messages that fit, preserving chronological order."""

    selected: list[IMContextMessage] = []
    used = 0
    for message in reversed(messages[-MAX_CONTEXT_MESSAGES:]):
        content = _trim_content(message.content)
        if not content:
            continue
        cost = ceil(
            estimate_text(
                f"{message.sender_name or message.sender_id}：{content}"
            )
            * 1.1
        )
        if selected and used + cost > max_tokens:
            break
        selected.append(
            IMContextMessage(
                message_id=message.message_id,
                sender_id=message.sender_id,
                sender_name=message.sender_name,
                sender_type=message.sender_type,
                content=content,
                timestamp_ms=message.timestamp_ms,
            )
        )
        used += cost
    selected.reverse()
    return selected


def _participants(messages: list[IMContextMessage]) -> str:
    names: list[str] = []
    seen: set[int] = set()
    for message in messages:
        if message.sender_type != "human":
            continue
        if message.sender_id in seen:
            continue
        seen.add(message.sender_id)
        names.append(message.sender_name or f"用户{message.sender_id}")
    return "、".join(names) if names else "用户"


def build_im_messages(
    *,
    mode: Literal["private", "shared"],
    context_messages: list[IMContextMessage],
    trigger_message_id: str,
    extra_system: str | None = None,
    max_context_tokens: int = 6000,
) -> list[Message]:
    """Return system + context messages for one IM-triggered reply."""

    system = load_persona(mode) + "\n\n" + current_date_line()
    if extra_system:
        system += "\n\n" + extra_system

    selected = _select_context(context_messages, max_tokens=max_context_tokens)
    _sanitize_stale_bot_history(selected)
    trigger_index = next(
        (
            index
            for index, message in enumerate(selected)
            if message.message_id == trigger_message_id
        ),
        None,
    )
    if trigger_index is None:
        # The worker should include the trigger, but never let a client omit it.
        trigger = next(
            (
                message
                for message in reversed(context_messages)
                if message.message_id == trigger_message_id
            ),
            context_messages[-1] if context_messages else None,
        )
        if trigger is not None:
            selected.append(
                IMContextMessage(
                    message_id=trigger.message_id,
                    sender_id=trigger.sender_id,
                    sender_name=trigger.sender_name,
                    sender_type=trigger.sender_type,
                    content=_trim_content(trigger.content),
                    timestamp_ms=trigger.timestamp_ms,
                )
            )
    else:
        # Drop anything after the trigger; stale context must not become the
        # message the model answers.
        selected = selected[: trigger_index + 1]

    messages: list[Message] = [Message(role="system", content=system)]

    if mode == "private":
        for message in selected:
            if not message.content:
                continue
            role = "assistant" if message.sender_type == "bot" else "user"
            messages.append(Message(role=role, content=message.content))
        if len(messages) == 1:
            messages.append(Message(role="user", content="你好"))
        messages.insert(
            len(messages) - 1,
            Message(role="system", content=_calibration_text()),
        )
        return messages

    lines = [
        f"参与者：{_participants(selected)}；你：小桁（AI 气氛组）",
        "以下是最近聊天记录，按时间从旧到新排列：",
    ]
    for message in selected:
        speaker = "小桁" if message.sender_type == "bot" else (
            message.sender_name or f"用户{message.sender_id}"
        )
        lines.append(f"{speaker}：{message.content}")
    lines.append("")
    lines.append("只回复最后一条被 @ 的消息，不要继续追问或长篇输出。")
    messages.insert(
        len(messages),
        Message(role="system", content=_calibration_text()),
    )
    messages.append(Message(role="user", content="\n".join(lines)))
    return messages


def shared_system_prompt() -> str:
    """Exposed for tests and prompt inspection."""

    return load_persona("shared")
