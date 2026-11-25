import pytest

from app.memory.extractor import MemoryCandidate
from app.memory.service import MemoryService, format_memory_block
from app.memory.store import MemoryHit
from app.storage.repositories import (
    ConversationRepository,
    MessageRepository,
    UserRepository,
)


class FakeEmbedding:
    async def embed(self, text: str) -> list[float]:
        return [0.1, 0.2, 0.3]


class FakeExtractor:
    def __init__(self, candidates: list[MemoryCandidate]) -> None:
        self.candidates = candidates

    async def extract(self, messages):
        return list(self.candidates)


class FakeStore:
    def __init__(self) -> None:
        self.inserted: list[dict] = []
        self.touched: list[dict] = []
        self.search_results: list[MemoryHit] = []

    async def find_similar(self, user_id, vector, *, min_similarity):
        return []

    async def insert(self, **kwargs) -> None:
        self.inserted.append(kwargs)

    async def touch(self, **kwargs) -> None:
        self.touched.append(kwargs)

    async def search(self, user_id, vector, *, top_k, min_similarity):
        return list(self.search_results)


def test_format_memory_block_labels_and_truncates():
    hits = [
        MemoryHit(1, "fact", "用户叫小明", 0.9, 0.7, 0.95),
        MemoryHit(2, "preference", "用户喜欢猫", 0.9, 0.7, 0.9),
    ]

    text = format_memory_block(hits, 600)

    assert "长期记忆" in text
    assert "[事实] 用户叫小明" in text
    assert "[偏好] 用户喜欢猫" in text


@pytest.mark.asyncio
async def test_memory_service_extracts_every_three_turns(app, settings):
    settings.memory_enabled = True
    settings.memory_extract_every_turns = 3
    settings.memory_extraction_max_messages = 6

    store = FakeStore()
    service = MemoryService(
        app.state.session_factory,
        store,
        FakeEmbedding(),
        FakeExtractor(
            [MemoryCandidate("preference", "用户喜欢猫", 0.9, 0.7)]
        ),
        settings,
    )

    async with app.state.session_factory() as session:
        user = await UserRepository(session).get_or_create(
            settings.dev_user_external_id
        )
        conversation = await ConversationRepository(session).create(
            user_id=user.id,
            title="memory-test",
        )
        messages = MessageRepository(session)
        for index in range(3):
            await messages.create(
                conversation_id=conversation.id,
                role="user",
                content=f"第 {index} 轮用户消息",
            )
            await messages.create(
                conversation_id=conversation.id,
                role="assistant",
                content=f"第 {index} 轮回复",
            )
        await session.commit()
        conversation_id = conversation.id
        user_id = user.id

    await service.maybe_extract(conversation_id, user_id)

    assert len(store.inserted) == 1
    assert store.inserted[0]["content"] == "用户喜欢猫"


@pytest.mark.asyncio
async def test_memory_service_does_not_extract_before_three_turns(app, settings):
    settings.memory_enabled = True
    settings.memory_extract_every_turns = 3

    store = FakeStore()
    service = MemoryService(
        app.state.session_factory,
        store,
        FakeEmbedding(),
        FakeExtractor(
            [MemoryCandidate("preference", "用户喜欢猫", 0.9, 0.7)]
        ),
        settings,
    )

    async with app.state.session_factory() as session:
        user = await UserRepository(session).get_or_create(
            settings.dev_user_external_id
        )
        conversation = await ConversationRepository(session).create(
            user_id=user.id,
            title="memory-test-2",
        )
        messages = MessageRepository(session)
        for _ in range(2):
            await messages.create(
                conversation_id=conversation.id,
                role="user",
                content="用户消息",
            )
            await messages.create(
                conversation_id=conversation.id,
                role="assistant",
                content="回复",
            )
        await session.commit()
        conversation_id = conversation.id
        user_id = user.id

    await service.maybe_extract(conversation_id, user_id)

    assert store.inserted == []


@pytest.mark.asyncio
async def test_memory_service_retrieve_formats_hits(app, settings):
    settings.memory_enabled = True
    store = FakeStore()
    store.search_results = [
        MemoryHit(1, "fact", "用户叫小明", 0.9, 0.7, 0.92)
    ]
    service = MemoryService(
        app.state.session_factory,
        store,
        FakeEmbedding(),
        FakeExtractor([]),
        settings,
    )

    text = await service.retrieve(1, "我叫什么")

    assert text is not None
    assert "用户叫小明" in text
