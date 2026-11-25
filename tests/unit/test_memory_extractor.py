import pytest

from app.config import Settings
from app.llm.base import Message
from app.llm.fake import FakeProvider
from app.memory.extractor import MemoryExtractor, parse_memory_candidates


def _settings() -> Settings:
    return Settings(
        memory_min_confidence=0.6,
        memory_max_content_chars=500,
    )


def test_parse_memory_candidates_accepts_code_fence():
    raw = """```json
    [
      {"kind": "fact", "content": "用户叫小明", "confidence": 0.9, "importance": 0.7},
      {"kind": "preference", "content": "用户喜欢猫", "confidence": 0.8, "importance": 0.6}
    ]
    ```"""

    candidates = parse_memory_candidates(raw)

    assert len(candidates) == 2
    assert candidates[0].kind == "fact"
    assert candidates[1].content == "用户喜欢猫"


def test_parse_memory_candidates_rejects_invalid_and_sensitive():
    raw = """[
      {"kind": "fact", "content": "用户叫小明", "confidence": 0.9},
      {"kind": "fact", "content": "用户的密码是 123456", "confidence": 0.9},
      {"kind": "unknown", "content": "用户喜欢猫", "confidence": 0.9}
    ]"""

    candidates = parse_memory_candidates(raw)

    assert len(candidates) == 1
    assert candidates[0].content == "用户叫小明"


@pytest.mark.asyncio
async def test_extractor_uses_provider_json():
    provider = FakeProvider(
        '[{"kind":"preference","content":"用户喜欢猫","confidence":0.9,"importance":0.7}]',
        delay=0.0,
    )
    extractor = MemoryExtractor(provider, _settings())

    candidates = await extractor.extract(
        [Message(role="user", content="我喜欢猫")]
    )

    assert len(candidates) == 1
    assert candidates[0].kind == "preference"
    assert candidates[0].content == "用户喜欢猫"
