import re
from dataclasses import dataclass

from app.orchestrator.token_counter import estimate_text

_UNIT_SPLIT = re.compile(r"(?<=[。！？.!?；;])\s*|\n+")


@dataclass(slots=True)
class TextChunk:
    index: int
    content: str
    token_count: int


def _split_units(text: str) -> list[str]:
    return [
        unit.strip()
        for unit in _UNIT_SPLIT.split(text)
        if unit and unit.strip()
    ]


def _take_prefix(text: str, max_tokens: int) -> tuple[str, str]:
    low, high = 0, len(text)
    while low < high:
        middle = (low + high + 1) // 2
        if estimate_text(text[:middle]) <= max_tokens:
            low = middle
        else:
            high = middle - 1
    return text[:low], text[low:]


def _expand_long_unit(unit: str, max_tokens: int) -> list[str]:
    pieces: list[str] = []
    while estimate_text(unit) > max_tokens:
        prefix, rest = _take_prefix(unit, max_tokens)
        if not prefix:
            break
        pieces.append(prefix)
        unit = rest
    if unit:
        pieces.append(unit)
    return pieces


def chunk_text(
    text: str,
    *,
    max_tokens: int = 500,
    overlap_tokens: int = 50,
) -> list[TextChunk]:
    normalized = text.replace("\r\n", "\n").strip()
    if not normalized:
        return []

    units: list[str] = []
    for unit in _split_units(normalized):
        if estimate_text(unit) > max_tokens:
            units.extend(_expand_long_unit(unit, max_tokens))
        else:
            units.append(unit)

    chunks: list[str] = []
    current: list[str] = []
    current_tokens = 0

    for unit in units:
        unit_tokens = estimate_text(unit)
        candidate_tokens = estimate_text("\n".join(current + [unit]))

        if current and candidate_tokens > max_tokens:
            chunks.append("\n".join(current).strip())

            overlap: list[str] = []
            overlap_count = 0
            for previous in reversed(current):
                previous_tokens = estimate_text(previous)
                if overlap_count + previous_tokens > overlap_tokens:
                    break
                overlap.insert(0, previous)
                overlap_count += previous_tokens

            current = overlap
            current_tokens = estimate_text("\n".join(current)) if current else 0
            candidate_tokens = estimate_text("\n".join(current + [unit]))

            if current and candidate_tokens > max_tokens:
                current = []
                current_tokens = 0

        current.append(unit)
        current_tokens = estimate_text("\n".join(current))

    if current:
        chunks.append("\n".join(current).strip())

    return [
        TextChunk(
            index=index,
            content=content,
            token_count=estimate_text(content),
        )
        for index, content in enumerate(chunks)
        if content
    ]
