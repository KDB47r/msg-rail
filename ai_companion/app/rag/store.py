import json
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.memory.store import vector_literal


@dataclass(slots=True)
class ChunkHit:
    document_id: int
    title: str
    source: str | None
    chunk_index: int
    content: str
    similarity: float


@dataclass(slots=True)
class DocumentSummary:
    id: int
    title: str
    source: str | None
    chunk_count: int
    created_at: datetime


class PgVectorRagStore:
    def __init__(self, session_factory: async_sessionmaker) -> None:
        self._session_factory = session_factory

    async def create_document(
        self,
        *,
        user_id: int,
        title: str,
        source: str | None,
        chunks: list[tuple[int, str, int, list[float]]],
        metadata: dict | None = None,
    ) -> int:
        insert_document = text(
            """
            INSERT INTO documents (user_id, title, source, metadata_json, created_at)
            VALUES (:user_id, :title, :source, CAST(:metadata AS json), now())
            RETURNING id
            """
        )
        insert_chunk = text(
            """
            INSERT INTO document_chunks (
              document_id, chunk_index, content, token_count, embedding,
              metadata_json, created_at
            ) VALUES (
              :document_id, :chunk_index, :content, :token_count,
              CAST(:embedding AS vector), CAST(:metadata AS json), now()
            )
            """
        )

        async with self._session_factory() as session:
            document_id = await session.scalar(
                insert_document,
                {
                    "user_id": user_id,
                    "title": title,
                    "source": source,
                    "metadata": json.dumps(metadata or {}, ensure_ascii=False),
                },
            )
            if document_id is None:
                raise RuntimeError("failed to create document")

            for chunk_index, content, token_count, embedding in chunks:
                await session.execute(
                    insert_chunk,
                    {
                        "document_id": document_id,
                        "chunk_index": chunk_index,
                        "content": content,
                        "token_count": token_count,
                        "embedding": vector_literal(embedding),
                        "metadata": "{}",
                    },
                )
            await session.commit()
            return int(document_id)

    async def search(
        self,
        user_id: int,
        query_vector: list[float],
        *,
        top_k: int,
        min_similarity: float,
    ) -> list[ChunkHit]:
        sql = text(
            """
            SELECT c.document_id, d.title, d.source, c.chunk_index, c.content,
                   1 - (c.embedding <=> CAST(:query AS vector)) AS similarity
            FROM document_chunks c
            JOIN documents d ON d.id = c.document_id
            WHERE d.user_id = :user_id
              AND c.embedding IS NOT NULL
              AND 1 - (c.embedding <=> CAST(:query AS vector)) >= :min_similarity
            ORDER BY c.embedding <=> CAST(:query AS vector)
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

        return [ChunkHit(**dict(row)) for row in rows]

    async def list_for_user(self, user_id: int) -> list[DocumentSummary]:
        sql = text(
            """
            SELECT d.id, d.title, d.source, d.created_at,
                   count(c.id) AS chunk_count
            FROM documents d
            LEFT JOIN document_chunks c ON c.document_id = d.id
            WHERE d.user_id = :user_id
            GROUP BY d.id, d.title, d.source, d.created_at
            ORDER BY d.created_at DESC
            """
        )
        async with self._session_factory() as session:
            rows = (
                await session.execute(sql, {"user_id": user_id})
            ).mappings().all()

        return [DocumentSummary(**dict(row)) for row in rows]

    async def delete(self, *, user_id: int, document_id: int) -> bool:
        delete_chunks = text(
            """
            DELETE FROM document_chunks
            WHERE document_id IN (
              SELECT id FROM documents
              WHERE id = :document_id AND user_id = :user_id
            )
            """
        )
        delete_document = text(
            "DELETE FROM documents WHERE id = :document_id AND user_id = :user_id"
        )
        async with self._session_factory() as session:
            await session.execute(
                delete_chunks,
                {"user_id": user_id, "document_id": document_id},
            )
            result = await session.execute(
                delete_document,
                {"user_id": user_id, "document_id": document_id},
            )
            await session.commit()
            return bool(result.rowcount)
