from datetime import datetime, timezone

import pytest

from app.rag.store import DocumentSummary


class FakeRagService:
    def __init__(self) -> None:
        self.ingested: list[dict] = []
        self.deleted: list[tuple[int, int]] = []

    async def ingest(self, *, user_id, title, source, content):
        self.ingested.append(
            {
                "user_id": user_id,
                "title": title,
                "source": source,
                "content": content,
            }
        )
        return {
            "id": 1,
            "title": title,
            "chunks": 2,
            "created_at": "2026-11-22T12:00:00+00:00",
        }

    async def list_documents(self, user_id):
        return [
            DocumentSummary(
                id=1,
                title="test.md",
                source="upload",
                chunk_count=2,
                created_at=datetime.now(timezone.utc),
            )
        ]

    async def delete(self, user_id, document_id):
        self.deleted.append((user_id, document_id))
        return True


@pytest.mark.asyncio
async def test_upload_list_delete_document(client, app, settings):
    fake = FakeRagService()
    app.state.rag_service = fake

    auth = {"Authorization": f"Bearer {settings.dev_token}"}

    response = await client.post(
        "/v1/documents",
        headers={
            **auth,
            "Content-Type": "text/markdown",
            "X-Filename": "test.md",
        },
        content="# 测试\n\n保修期为 12 个月。",
    )

    assert response.status_code == 201
    assert response.json()["title"] == "test.md"
    assert fake.ingested[0]["title"] == "test.md"
    assert "保修期" in fake.ingested[0]["content"]

    response = await client.get("/v1/documents", headers=auth)
    assert response.status_code == 200
    assert response.json()["documents"][0]["title"] == "test.md"

    response = await client.delete("/v1/documents/1", headers=auth)
    assert response.status_code == 200
    assert fake.deleted[0][1] == 1


@pytest.mark.asyncio
async def test_upload_rejects_invalid_extension(client, app, settings):
    app.state.rag_service = FakeRagService()
    auth = {"Authorization": f"Bearer {settings.dev_token}"}

    response = await client.post(
        "/v1/documents",
        headers={
            **auth,
            "Content-Type": "application/octet-stream",
            "X-Filename": "test.pdf",
        },
        content=b"not supported",
    )

    assert response.status_code == 400
