import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.config import Settings
from app.main import create_app
from app.storage import models  # noqa: F401 - register tables on Base.metadata
from app.storage.db import Base


@pytest.fixture
def settings() -> Settings:
    return Settings(
        _env_file=None,
        dev_token="test-token",
        database_url="sqlite+aiosqlite:///:memory:",
        llm_provider="fake",
        fake_response="你好呀，我是小火。",
        fake_delay_seconds=0.0,
        sse_heartbeat_seconds=0.0,
        tools_enabled=False,
        memory_enabled=False,
        rag_enabled=False,
    )


@pytest_asyncio.fixture
async def app(settings: Settings):
    application = create_app(settings)
    async with application.state.engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    yield application

    await application.state.engine.dispose()


@pytest_asyncio.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as http_client:
        yield http_client
