from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker


@dataclass(slots=True)
class MemoryHit:
    id: int
    kind: str
    content: str
    confidence: float
    importance: float
    similarity: float


@dataclass(slots=True)
class MemoryRecord:
    id: int
    kind: str
    content: str
    confidence: float
    importance: float
    status: str
    created_at: datetime
    last_seen_at: datetime


def vector_literal(vector: list[float]) -> str:
    return "[" + ",".join(f"{value:.8f}" for value in vector) + "]"


class PgVectorMemoryStore:
    """Raw-SQL pgvector store.

    The ORM model keeps ``embedding`` as TEXT so SQLite tests can create tables;
    on PostgreSQL the migration changes it to ``vector(1024)`` and all vector
    reads/writes go through explicit CASTs here.
    """

    def __init__(self, session_factory: async_sessionmaker) -> None:
        self._session_factory = session_factory

    async def search(
        self,
        user_id: int,
        query_vector: list[float],
        *,
        top_k: int,
        min_similarity: float,
    ) -> list[MemoryHit]:
        sql = text(
            """
            SELECT id, kind, content, confidence, importance,
                   1 - (embedding <=> CAST(:query AS vector)) AS similarity
            FROM memories
            WHERE user_id = :user_id
              AND status = 'active'
              AND embedding IS NOT NULL
              AND 1 - (embedding <=> CAST(:query AS vector)) >= :min_similarity
            ORDER BY embedding <=> CAST(:query AS vector)
            LIMIT :top_k
            """
        )
        async with self._session_factory() as session:
            rows = (
                await session.execute(
                    sql,
                    {
                        "user_id": user_id,
                        "query": vector_literal(query_vector),
                        "min_similarity": min_similarity,
                        "top_k": top_k,
                    },
                )
            ).mappings().all()

        return [MemoryHit(**dict(row)) for row in rows]

    async def find_similar(
        self,
        user_id: int,
        query_vector: list[float],
        *,
        min_similarity: float,
    ) -> list[MemoryHit]:
        return await self.search(
            user_id,
            query_vector,
            top_k=1,
            min_similarity=min_similarity,
        )

    async def insert(
        self,
        *,
        user_id: int,
        kind: str,
        content: str,
        content_hash: str,
        embedding: list[float],
        source_message_id: str | None,
        confidence: float,
        importance: float,
    ) -> None:
        sql = text(
            """
            INSERT INTO memories (
              user_id, kind, content, content_hash, embedding,
              source_message_id, confidence, importance, status,
              created_at, last_seen_at
            ) VALUES (
              :user_id, :kind, :content, :content_hash,
              CAST(:embedding AS vector),
              :source_message_id, :confidence, :importance, 'active',
              now(), now()
            )
            ON CONFLICT (user_id, content_hash) DO UPDATE SET
              content = EXCLUDED.content,
              embedding = EXCLUDED.embedding,
              confidence = GREATEST(memories.confidence, EXCLUDED.confidence),
              importance = GREATEST(memories.importance, EXCLUDED.importance),
              last_seen_at = now()
            """
        )
        async with self._session_factory() as session:
            await session.execute(
                sql,
                {
                    "user_id": user_id,
                    "kind": kind,
                    "content": content,
                    "content_hash": content_hash,
                    "embedding": vector_literal(embedding),
                    "source_message_id": source_message_id,
                    "confidence": confidence,
                    "importance": importance,
                },
            )
            await session.commit()

    async def touch(
        self,
        *,
        user_id: int,
        memory_id: int,
        confidence: float,
    ) -> None:
        sql = text(
            """
            UPDATE memories
            SET last_seen_at = now(),
                confidence = GREATEST(confidence, :confidence)
            WHERE id = :memory_id AND user_id = :user_id
            """
        )
        async with self._session_factory() as session:
            await session.execute(
                sql,
                {
                    "user_id": user_id,
                    "memory_id": memory_id,
                    "confidence": confidence,
                },
            )
            await session.commit()

    async def list_for_user(self, user_id: int) -> list[MemoryRecord]:
        sql = text(
            """
            SELECT id, kind, content, confidence, importance, status,
                   created_at, last_seen_at
            FROM memories
            WHERE user_id = :user_id AND status = 'active'
            ORDER BY created_at DESC
            """
        )
        async with self._session_factory() as session:
            rows = (
                await session.execute(sql, {"user_id": user_id})
            ).mappings().all()

        return [MemoryRecord(**dict(row)) for row in rows]

    async def delete(self, *, user_id: int, memory_id: int) -> bool:
        sql = text(
            "DELETE FROM memories WHERE id = :memory_id AND user_id = :user_id"
        )
        async with self._session_factory() as session:
            result = await session.execute(
                sql,
                {"user_id": user_id, "memory_id": memory_id},
            )
            await session.commit()
            return bool(result.rowcount)

    async def delete_all(self, user_id: int) -> int:
        sql = text("DELETE FROM memories WHERE user_id = :user_id")
        async with self._session_factory() as session:
            result = await session.execute(sql, {"user_id": user_id})
            await session.commit()
            return int(result.rowcount or 0)
