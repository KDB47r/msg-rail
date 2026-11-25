import pytest


@pytest.mark.asyncio
async def test_healthz(client):
    response = await client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_readyz(client):
    response = await client.get("/readyz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_index_is_served(client):
    response = await client.get("/")

    assert response.status_code == 200
    assert "AI Companion" in response.text
