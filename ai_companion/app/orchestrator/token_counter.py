import json
import math
from collections.abc import Iterable

from app.llm.base import Message, ToolSpec

MESSAGE_OVERHEAD = 4
TOOL_SPEC_OVERHEAD = 8


def _is_cjk(char: str) -> bool:
    code = ord(char)
    return (
        0x3400 <= code <= 0x4DBF
        or 0x4E00 <= code <= 0x9FFF
        or 0xF900 <= code <= 0xFAFF
    )


def estimate_text(text: str) -> int:
    """Rough token estimate: CJK ~1 token/char, other ~4 chars/token.

    This is intentionally approximate. The abstraction lets P2/P3 swap in a real
    tokenizer later without changing the context builder.
    """

    if not text:
        return 0

    cjk = sum(1 for char in text if _is_cjk(char))
    other = len(text) - cjk
    return cjk + math.ceil(other / 4)


def estimate_message(message: Message) -> int:
    return estimate_text(message.content) + MESSAGE_OVERHEAD


def estimate_messages(messages: list[Message]) -> int:
    return sum(estimate_message(message) for message in messages)


def estimate_tool_specs(tools: Iterable[ToolSpec]) -> int:
    total = 0
    for tool in tools:
        payload = json.dumps(
            {
                "name": tool.name,
                "description": tool.description,
                "parameters": tool.parameters,
            },
            ensure_ascii=False,
        )
        total += estimate_text(payload) + TOOL_SPEC_OVERHEAD
    return total


def truncate_to_tokens(text: str, max_tokens: int) -> str:
    """Best-effort truncation using the same rough estimator as the budget."""

    if max_tokens <= 0 or estimate_text(text) <= max_tokens:
        return text

    low, high = 0, len(text)
    while low < high:
        middle = (low + high + 1) // 2
        if estimate_text(text[:middle]) <= max_tokens:
            low = middle
        else:
            high = middle - 1

    return text[:low] + "\n...[truncated]"
