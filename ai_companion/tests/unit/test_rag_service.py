import pytest

from app.config import Settings
from app.rag.service import RagService, TooManyChunks, format_rag_block
from app.rag.store import ChunkHit


class FakeEmbedding:
    async def embed(self, text: str) -> list[float]:
        return [0.1, 0.2]

    async def embed_batch(
        self,
        texts: list[str],
        *,
        batch_size: int = 10,
    ) -> list[list[float]]:
        return [[0.1, 0.2] for _ in texts]


class FakeStore:
    def __init__(self) -> None:
        self.created = None
        self.search_results: list[ChunkHit] = []
        self.deleted = []

    async def create_document(self, **kwargs):
        self.created = kwargs
        return 42

    async def search(self, user_id, query_vector, *, top_k, min_similarity):
        return list(self.search_results)

    async def list_for_user(self, user_id):
        return []

    async def delete(self, *, user_id, document_id):
        self.deleted.append((user_id, document_id))
        return True


def _settings(**overrides) -> Settings:
    values = {
        "rag_enabled": True,
        "rag_chunk_tokens": 100,
        "rag_chunk_overlap": 10,
        "rag_max_chunks": 10,
    }
    values.update(overrides)
    return Settings(**values)


def test_format_rag_block_includes_source():
    hits = [
        ChunkHit(
            document_id=1,
            title="产品手册",
            source="upload",
            chunk_index=2,
            content="保修期为 12 个月。",
            similarity=0.8,
        )
    ]

    text = format_rag_block(hits, 1200)

    assert "参考资料" in text
    assert "产品手册#3" in text
    assert "保修期为 12 个月" in text


@pytest.mark.asyncio
async def test_rag_ingest_chunks_and_embeds():
    store = FakeStore()
    service = RagService(store, FakeEmbedding(), _settings())

    result = await service.ingest(
        user_id=1,
        title="test.md",
        source="upload",
        content="这是第一段。\n\n这是第二段。",
    )

    assert result["id"] == 42
    assert result["chunks"] >= 1
    assert store.created is not None
    chunks = store.created["chunks"]
    assert len(chunks) == result["chunks"]
    assert all(len(item) == 4 for item in chunks)


@pytest.mark.asyncio
async def test_rag_ingest_rejects_too_many_chunks():
    service = RagService(
        FakeStore(),
        FakeEmbedding(),
        _settings(rag_max_chunks=1, rag_chunk_tokens=5),
    )

    with pytest.raises(TooManyChunks):
        await service.ingest(
            user_id=1,
            title="x.md",
            source="upload",
            content="这是第一句。这是第二句。这是第三句。",
        )


@pytest.mark.asyncio
async def test_rag_retrieve_formats_hits():
    store = FakeStore()
    store.search_results = [
        ChunkHit(1, "产品手册", "upload", 0, "保修期为 12 个月。", 0.9)
    ]
    service = RagService(store, FakeEmbedding(), _settings())

    text = await service.retrieve(1, "保修多久")

    assert text is not None
    assert "保修期为 12 个月" in text
