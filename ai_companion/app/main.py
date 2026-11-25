import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.api.chat import router as chat_router
from app.api.conversations import router as conversations_router
from app.api.documents import router as documents_router
from app.api.health import router as health_router
from app.api.internal import router as internal_router
from app.api.memories import router as memories_router
from app.config import Settings, get_settings
from app.llm.registry import build_provider
from app.mcp.manager import MCPClientManager
from app.memory.embedding import EmbeddingProvider
from app.memory.extractor import MemoryExtractor
from app.memory.service import MemoryService
from app.memory.store import PgVectorMemoryStore
from app.observability.logging import configure_logging
from app.orchestrator.summary_service import SummaryService
from app.rag.service import RagService
from app.rag.store import PgVectorRagStore
from app.storage.db import build_engine, build_session_factory
from app.tools.local import register_local_tools
from app.tools.recorder import ToolCallRecorder
from app.tools.registry import ToolRegistry

logger = logging.getLogger(__name__)

WEB_DIR = Path(__file__).resolve().parent.parent / "web"


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    try:
        await app.state.mcp_manager.start()
    except Exception:
        logger.exception("failed to start MCP manager")

    yield

    await app.state.mcp_manager.close()
    await app.state.engine.dispose()


def create_app(settings: Settings | None = None) -> FastAPI:
    configure_logging()
    settings = settings or get_settings()

    app = FastAPI(title=settings.app_name, lifespan=lifespan)
    app.state.settings = settings
    app.state.provider = build_provider(settings)
    app.state.engine = build_engine(settings.database_url, settings.db_echo)
    app.state.session_factory = build_session_factory(app.state.engine)

    summary_settings = settings.model_copy(
        update={
            "llm_model": settings.summary_model or settings.llm_model,
            "llm_max_tokens": settings.summary_max_tokens,
            "llm_temperature": settings.summary_temperature,
        }
    )
    app.state.summary_provider = build_provider(summary_settings)
    app.state.summary_service = SummaryService(
        app.state.session_factory,
        app.state.summary_provider,
        settings,
    )

    app.state.tool_registry = ToolRegistry(settings)
    register_local_tools(app.state.tool_registry)
    app.state.tool_recorder = ToolCallRecorder(app.state.session_factory)
    app.state.mcp_manager = MCPClientManager(settings, app.state.tool_registry)

    memory_settings = settings.model_copy(
        update={
            "llm_model": settings.memory_extraction_model or settings.llm_model,
            "llm_temperature": settings.memory_extraction_temperature,
            "llm_max_tokens": 512,
        }
    )
    app.state.memory_provider = build_provider(memory_settings)
    app.state.embedding_provider = EmbeddingProvider(settings)
    app.state.memory_store = PgVectorMemoryStore(app.state.session_factory)
    app.state.memory_service = MemoryService(
        app.state.session_factory,
        app.state.memory_store,
        app.state.embedding_provider,
        MemoryExtractor(app.state.memory_provider, settings),
        settings,
    )

    app.state.rag_store = PgVectorRagStore(app.state.session_factory)
    app.state.rag_service = RagService(
        app.state.rag_store,
        app.state.embedding_provider,
        settings,
    )

    app.include_router(health_router)
    app.include_router(internal_router)
    app.include_router(chat_router)
    app.include_router(conversations_router)
    app.include_router(memories_router)
    app.include_router(documents_router)

    if WEB_DIR.exists():
        app.mount("/", StaticFiles(directory=WEB_DIR, html=True), name="web")

    return app
