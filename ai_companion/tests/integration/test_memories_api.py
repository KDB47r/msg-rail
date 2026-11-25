import pytest

from app.storage.models import Memory
from app.storage.repositories import UserRepository


@pytest.mark.asyncio
async def test_list_and_delete_memories(client, app, settings):
    settings.memory_enabled = True

    async with app.state.session_factory() as session:
        user = await UserRepository(session).get_or_create(
            settings.dev_user_external_id
        )
        memory = Memory(
            user_id=user.id,
            kind="fact",
            content="用户叫小明",
            content_hash="memory-api-hash-1",
            confidence=0.9,
            importance=0.7,
            status="active",
        )
        session.add(memory)
        await session.commit()
        await session.refresh(memory)
        memory_id = memory.id

    headers = {"Authorization": f"Bearer {settings.dev_token}"}

    response = await client.get("/v1/memories", headers=headers)
    assert response.status_code == 200
    memories = response.json()["memories"]
    assert len(memories) == 1
    assert memories[0]["content"] == "用户叫小明"

    response = await client.delete(
        f"/v1/memories/{memory_id}",
        headers=headers,
    )
    assert response.status_code == 200
    assert response.json() == {"status": "deleted"}

    response = await client.get("/v1/memories", headers=headers)
    assert response.json()["memories"] == []


@pytest.mark.asyncio
async def test_delete_all_memories(client, app, settings):
    settings.memory_enabled = True

    async with app.state.session_factory() as session:
        user = await UserRepository(session).get_or_create(
            settings.dev_user_external_id
        )
        session.add_all(
            [
                Memory(
                    user_id=user.id,
                    kind="fact",
                    content="用户叫小明",
                    content_hash="memory-api-hash-2",
                ),
                Memory(
                    user_id=user.id,
                    kind="preference",
                    content="用户喜欢猫",
                    content_hash="memory-api-hash-3",
                ),
            ]
        )
        await session.commit()

    headers = {"Authorization": f"Bearer {settings.dev_token}"}
    response = await client.delete("/v1/memories", headers=headers)

    assert response.status_code == 200
    assert response.json()["deleted"] == 2
