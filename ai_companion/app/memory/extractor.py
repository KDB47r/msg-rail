import json
import logging
from dataclasses import dataclass
from pathlib import Path

from app.config import Settings
from app.llm.base import LLMProvider, Message

logger = logging.getLogger(__name__)

MEMORY_PROMPT_PATH = Path(__file__).resolve().parent.parent / "prompts" / "memory.md"
DEFAULT_MEMORY_PROMPT = (
    "从对话中提取用户的稳定事实、偏好和重要事件，输出 JSON 数组；"
    "不要提取寒暄、天气、敏感信息或推断。"
)

ALLOWED_KINDS = {"fact", "preference", "event"}
SENSITIVE_MARKERS = (
    "密码",
    "api key",
    "api_key",
    "身份证",
    "银行卡",
    "信用卡",
    "cvv",
)


@dataclass(slots=True)
class MemoryCandidate:
    kind: str
    content: str
    confidence: float
    importance: float


def load_memory_prompt() -> str:
    try:
        return MEMORY_PROMPT_PATH.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return DEFAULT_MEMORY_PROMPT


def _strip_code_fence(text: str) -> str:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("\n", 1)[1] if "\n" in cleaned else ""
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
    return cleaned.strip()


def parse_memory_candidates(
    raw: str,
    *,
    min_confidence: float = 0.6,
    max_content_chars: int = 500,
) -> list[MemoryCandidate]:
    text = _strip_code_fence(raw)
    start = text.find("[")
    end = text.rfind("]")
    if start < 0 or end < start:
        return []

    try:
        payload = json.loads(text[start : end + 1])
    except json.JSONDecodeError:
        return []

    if not isinstance(payload, list):
        return []

    candidates: list[MemoryCandidate] = []
    seen: set[str] = set()

    for item in payload:
        if not isinstance(item, dict):
            continue

        kind = str(item.get("kind", "")).strip()
        content = str(item.get("content", "")).strip()
        if kind not in ALLOWED_KINDS or not content:
            continue
        if len(content) > max_content_chars:
            content = content[:max_content_chars]
        if any(marker in content.lower() for marker in SENSITIVE_MARKERS):
            continue

        try:
            confidence = float(item.get("confidence", 0.8))
            importance = float(item.get("importance", 0.5))
        except (TypeError, ValueError):
            continue

        confidence = min(1.0, max(0.0, confidence))
        importance = min(1.0, max(0.0, importance))
        if confidence < min_confidence:
            continue

        key = content.lower()
        if key in seen:
            continue
        seen.add(key)

        candidates.append(
            MemoryCandidate(
                kind=kind,
                content=content,
                confidence=confidence,
                importance=importance,
            )
        )

        if len(candidates) >= 5:
            break

    return candidates


class MemoryExtractor:
    def __init__(self, provider: LLMProvider, settings: Settings) -> None:
        self._provider = provider
        self._settings = settings

    async def extract(self, messages: list[Message]) -> list[MemoryCandidate]:
        dialogue = "\n".join(
            f"{message.role}: {message.content}" for message in messages
        )
        parts: list[str] = []

        async for event in self._provider.stream(
            [
                Message(role="system", content=load_memory_prompt()),
                Message(
                    role="user",
                    content=f"最近对话：\n{dialogue}\n\n请输出记忆 JSON 数组。",
                ),
            ]
        ):
            if event.type == "text_delta":
                parts.append(event.delta)
            elif event.type == "end":
                break

        return parse_memory_candidates(
            "".join(parts),
            min_confidence=self._settings.memory_min_confidence,
            max_content_chars=self._settings.memory_max_content_chars,
        )
