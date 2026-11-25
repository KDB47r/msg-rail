import asyncio
import contextlib
import logging
import time
from collections.abc import AsyncIterator

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy.exc import SQLAlchemyError

from app.api.deps import get_current_user
from app.config import Settings
from app.llm.base import LLMProvider, ToolSpec, Usage
from app.orchestrator.agent_loop import stream_turn as agent_stream_turn
from app.orchestrator.chat_service import stream_turn
from app.orchestrator.context_builder import ContextTooLarge
from app.orchestrator.events import format_sse, heartbeat
from app.orchestrator.summary_service import SummaryService
from app.orchestrator.turn_service import (
    ConversationNotFound,
    PreparedTurn,
    ReplayTurn,
    TurnInProgress,
    TurnService,
)
from app.storage.models import User

logger = logging.getLogger(__name__)

router = APIRouter(tags=["chat"])

SSE_HEADERS = {
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",
}


class ChatRequest(BaseModel):
    conversation_id: str | None = Field(default=None, max_length=128)
    message: str = Field(min_length=1, max_length=8000)
    client_message_id: str | None = Field(default=None, max_length=128)
    persona_id: str = Field(default="default", max_length=64)


@router.post("/v1/chat/stream")
async def chat_stream(
    payload: ChatRequest,
    request: Request,
    user: User = Depends(get_current_user),
) -> StreamingResponse:
    settings: Settings = request.app.state.settings
    provider: LLMProvider = request.app.state.provider
    service = TurnService(request.app.state.session_factory, settings)

    tools: list[ToolSpec] = []
    tool_spec_tokens = 0
    allowed_tools = settings.tool_allowlist("private")
    if settings.tools_enabled:
        tools = request.app.state.tool_registry.list_specs(allowed_tools)
        tool_spec_tokens = request.app.state.tool_registry.spec_token_estimate(
            allowed_tools
        )

    memory_text = None
    rag_text = None
    if settings.memory_enabled or settings.rag_enabled:
        query_vector = None
        try:
            query_vector = await request.app.state.embedding_provider.embed(
                payload.message
            )
        except Exception:
            logger.exception("query embedding failed user_id=%s", user.id)

        if query_vector is not None:
            if settings.memory_enabled:
                memory_text = await request.app.state.memory_service.retrieve(
                    user.id,
                    payload.message,
                    query_vector=query_vector,
                )
            if settings.rag_enabled:
                rag_text = await request.app.state.rag_service.retrieve(
                    user.id,
                    payload.message,
                    query_vector=query_vector,
                )

    try:
        prepared = await service.prepare(
            user_id=user.id,
            conversation_id=payload.conversation_id,
            message=payload.message,
            client_message_id=payload.client_message_id,
            persona_id=payload.persona_id,
            tool_spec_tokens=tool_spec_tokens,
            memory_text=memory_text,
            rag_text=rag_text,
        )
    except ConversationNotFound as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="conversation not found",
        ) from exc
    except TurnInProgress as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="AI request already in progress",
        ) from exc
    except ContextTooLarge as exc:
        raise HTTPException(
            status_code=status.HTTP_413_CONTENT_TOO_LARGE,
            detail="context too large",
        ) from exc
    except SQLAlchemyError as exc:
        logger.exception("failed to prepare chat turn")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="database unavailable",
        ) from exc

    if isinstance(prepared, ReplayTurn):
        return StreamingResponse(
            _replay_events(prepared),
            media_type="text/event-stream",
            headers=SSE_HEADERS,
        )

    return StreamingResponse(
        _new_turn_events(
            service,
            prepared,
            provider,
            request,
            settings,
            tools,
            allowed_tools,
        ),
        media_type="text/event-stream",
        headers=SSE_HEADERS,
    )


async def _replay_events(turn: ReplayTurn) -> AsyncIterator[str]:
    event_id = 0

    def event(name: str, data: dict[str, object]) -> str:
        nonlocal event_id
        event_id += 1
        return format_sse(name, data, event_id)

    usage = _usage_dict(turn.usage)

    yield event(
        "message_start",
        {
            "conversation_id": turn.conversation_id,
            "message_id": turn.assistant_message_id,
            "run_id": turn.run_id,
            "created_at": turn.created_at,
        },
    )
    if turn.content:
        yield event("content_delta", {"delta": turn.content})
    if usage is not None:
        yield event("usage", usage)
    yield event(
        "message_end",
        {
            "finish_reason": "stop",
            "message_id": turn.assistant_message_id,
            "run_id": turn.run_id,
            "usage": usage,
            "replayed": True,
            "status": turn.status,
        },
    )


async def _new_turn_events(
    service: TurnService,
    prepared: PreparedTurn,
    provider: LLMProvider,
    request: Request,
    settings: Settings,
    tools: list[ToolSpec],
    allowed_tools: set[str] | None,
) -> AsyncIterator[str]:
    event_id = 0
    status = "aborted"
    error_code: str | None = None
    finish_reason = "stop"
    usage_data: dict[str, object] | None = None
    chunks: list[str] = []
    started = time.perf_counter()
    first_delta_at: float | None = None

    def event(name: str, data: dict[str, object]) -> str:
        nonlocal event_id
        event_id += 1
        return format_sse(name, data, event_id)

    if settings.tools_enabled and tools:
        events = agent_stream_turn(
            provider,
            prepared.messages,
            tools,
            request.app.state.tool_registry,
            request.app.state.tool_recorder,
            conversation_id=prepared.conversation_id,
            message_id=prepared.assistant_message_id,
            run_id=prepared.run_id,
            created_at=prepared.created_at,
            settings=settings,
            allowed_tools=allowed_tools,
        )
    else:
        events = stream_turn(
            provider,
            prepared.messages,
            conversation_id=prepared.conversation_id,
            message_id=prepared.assistant_message_id,
            run_id=prepared.run_id,
            created_at=prepared.created_at,
            heartbeat_seconds=settings.sse_heartbeat_seconds,
        )

    try:
        async for item in events:
            if await request.is_disconnected():
                status = "aborted"
                break

            if item is None:
                yield heartbeat()
                continue

            if item.event == "content_delta":
                if first_delta_at is None:
                    first_delta_at = time.perf_counter()
                delta = item.data.get("delta")
                if isinstance(delta, str):
                    chunks.append(delta)
            elif item.event == "usage":
                usage_data = item.data
            elif item.event == "message_end":
                status = "completed"
                end_usage = item.data.get("usage")
                if isinstance(end_usage, dict):
                    usage_data = end_usage
                finish_reason = str(item.data.get("finish_reason") or "stop")
            elif item.event == "error":
                status = "failed"
                error_code = str(item.data.get("code") or "INTERNAL_ERROR")

            yield event(item.event, item.data)
    except Exception:
        logger.exception("unexpected chat stream error run_id=%s", prepared.run_id)
        status = "failed"
        error_code = "INTERNAL_ERROR"
        yield event(
            "error",
            {
                "code": error_code,
                "message": "服务内部错误",
                "retryable": False,
                "run_id": prepared.run_id,
            },
        )
    finally:
        content = "".join(chunks)
        ttft_ms = (
            int((first_delta_at - started) * 1000)
            if first_delta_at is not None
            else None
        )
        latency_ms = int((time.perf_counter() - started) * 1000)
        await _persist_safely(
            service.finish(
                prepared=prepared,
                content=content,
                status=status,
                usage=usage_data,
                ttft_ms=ttft_ms,
                latency_ms=latency_ms,
                error_code=error_code,
            )
        )

        if prepared.needs_summary and settings.summary_enabled:
            summary_service: SummaryService = request.app.state.summary_service
            if settings.summary_sync:
                await summary_service.maybe_summarize(prepared.conversation_id)
            else:
                summary_service.schedule(prepared.conversation_id)

        if settings.memory_enabled:
            memory_service = request.app.state.memory_service
            if settings.memory_sync:
                await memory_service.maybe_extract(
                    prepared.conversation_id,
                    prepared.user_id,
                )
            else:
                memory_service.schedule(
                    prepared.conversation_id,
                    prepared.user_id,
                )


async def _persist_safely(coro) -> None:
    task = asyncio.create_task(coro)
    try:
        await asyncio.shield(task)
    except asyncio.CancelledError:
        with contextlib.suppress(Exception):
            await task
        raise
    except Exception:
        logger.exception("failed to persist chat turn")


def _usage_dict(usage: Usage | None) -> dict[str, int] | None:
    if usage is None:
        return None
    return {
        "input_tokens": usage.input_tokens,
        "output_tokens": usage.output_tokens,
        "cached_tokens": usage.cached_tokens,
    }
