"""Internal endpoint used by MsgRail's AI Worker.

MsgRail is the source of truth for IM messages. This endpoint deliberately does
not create AI Companion conversations or store A/B history: it receives the
last messages, builds a prompt, streams the answer, and returns.
"""

import logging
import time
from typing import Literal

from fastapi import APIRouter, Header, HTTPException, Request, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.config import Settings
from app.llm.base import LLMProvider, ToolSpec
from app.orchestrator.agent_loop import stream_turn as agent_stream_turn
from app.orchestrator.chat_service import stream_turn
from app.orchestrator.events import format_sse, heartbeat
from app.orchestrator.im_context import (
    IMContextMessage,
    build_im_messages,
)
from app.security.auth import check_bearer_token
from app.storage.repositories import UserRepository

logger = logging.getLogger(__name__)

router = APIRouter(tags=["internal"])

SSE_HEADERS = {
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",
}


class ContextMessage(BaseModel):
    message_id: str = Field(default="", max_length=160)
    sender_id: int
    sender_name: str = Field(default="", max_length=128)
    sender_type: Literal["human", "bot"] = "human"
    content: str = Field(default="", max_length=8000)
    timestamp_ms: int = 0


class InternalAgentRequest(BaseModel):
    request_id: str = Field(min_length=1, max_length=128)
    mode: Literal["private", "shared"]
    user_id: str | None = Field(default=None, max_length=128)
    session_id: str = Field(min_length=1, max_length=160)
    bot_user_id: int
    trigger_message_id: str = Field(min_length=1, max_length=160)
    trigger_sender_id: int
    trigger_sender_name: str = Field(default="", max_length=128)
    context_messages: list[ContextMessage] = Field(
        min_length=1,
        max_length=40,
    )
    persona_id: str = Field(default="xiaoheng", max_length=64)
    idempotency_key: str | None = Field(default=None, max_length=160)


class _ImToolRecorder:
    """Structured tool audit for the stateless IM endpoint.

    We intentionally do not store arguments or web page contents here; those
    can contain private chat or untrusted web text.
    """

    async def record(self, *, run_id, step, call, result) -> None:
        logger.info(
            "im_tool_call job_id=%s step=%s tool=%s server=%s status=%s "
            "latency_ms=%s error_code=%s",
            run_id,
            step,
            call.name,
            result.server,
            result.status,
            result.latency_ms,
            result.error_code,
        )


@router.post("/internal/v1/agent/stream")
async def internal_agent_stream(
    payload: InternalAgentRequest,
    request: Request,
    authorization: str | None = Header(default=None),
) -> StreamingResponse:
    settings: Settings = request.app.state.settings
    check_bearer_token(authorization, settings.internal_token)

    if payload.mode == "private" and payload.trigger_sender_id <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="trigger_sender_id required for private mode",
        )

    context = [
        IMContextMessage(
            message_id=item.message_id,
            sender_id=item.sender_id,
            sender_name=item.sender_name,
            sender_type=item.sender_type,
            content=item.content,
            timestamp_ms=item.timestamp_ms,
        )
        for item in payload.context_messages
    ]
    trigger_content = next(
        (
            item.content
            for item in reversed(context)
            if item.message_id == payload.trigger_message_id
        ),
        context[-1].content if context else "",
    )

    extra_system = None
    if payload.mode == "private":
        extra_system = await _retrieve_private_context(
            request,
            payload,
            trigger_content=trigger_content,
        )

    max_context_tokens = max(
        1000,
        settings.context_window_tokens
        - settings.llm_max_tokens
        - 2000,
    )
    messages = build_im_messages(
        mode=payload.mode,
        context_messages=context,
        trigger_message_id=payload.trigger_message_id,
        extra_system=extra_system,
        max_context_tokens=max_context_tokens,
    )

    provider: LLMProvider = request.app.state.provider
    tools: list[ToolSpec] = []
    profile = "private" if payload.mode == "private" else "shared"
    allowed_tools = settings.tool_allowlist(profile)
    if settings.tools_enabled:
        tools = request.app.state.tool_registry.list_specs(allowed_tools)

    if tools:
        source = agent_stream_turn(
            provider,
            messages,
            tools,
            request.app.state.tool_registry,
            _ImToolRecorder(),
            conversation_id=payload.session_id,
            message_id=payload.request_id,
            run_id=payload.request_id,
            created_at=_now_iso(),
            settings=settings,
            allowed_tools=allowed_tools,
        )
    else:
        source = stream_turn(
            provider,
            messages,
            conversation_id=payload.session_id,
            message_id=payload.request_id,
            run_id=payload.request_id,
            created_at=_now_iso(),
            heartbeat_seconds=settings.sse_heartbeat_seconds,
        )

    return StreamingResponse(
        _event_stream(source, request, payload),
        media_type="text/event-stream",
        headers=SSE_HEADERS,
    )


async def _retrieve_private_context(
    request: Request,
    payload: InternalAgentRequest,
    *,
    trigger_content: str,
) -> str | None:
    settings: Settings = request.app.state.settings
    if not (settings.memory_enabled or settings.rag_enabled):
        return None
    if not trigger_content.strip():
        return None

    external_id = payload.user_id or f"im:{payload.trigger_sender_id}"
    if external_id.isdigit():
        external_id = f"im:{external_id}"

    user_id: int | None = None
    try:
        async with request.app.state.session_factory() as session:
            repo = UserRepository(session)
            user = await repo.get_or_create(
                external_id,
                display_name=payload.trigger_sender_name or "IM User",
            )
            await session.commit()
            user_id = user.id
    except Exception:
        logger.exception(
            "failed to resolve internal IM user external_id=%s",
            external_id,
        )
        return None

    query_vector = None
    try:
        query_vector = await request.app.state.embedding_provider.embed(
            trigger_content
        )
    except Exception:
        logger.exception("internal query embedding failed user_id=%s", user_id)

    blocks: list[str] = []
    if settings.memory_enabled:
        try:
            memory_text = await request.app.state.memory_service.retrieve(
                user_id,
                trigger_content,
                query_vector=query_vector,
            )
            if memory_text:
                blocks.append(memory_text)
        except Exception:
            logger.exception("internal memory retrieval failed user_id=%s", user_id)
    if settings.rag_enabled:
        try:
            rag_text = await request.app.state.rag_service.retrieve(
                user_id,
                trigger_content,
                query_vector=query_vector,
            )
            if rag_text:
                blocks.append(rag_text)
        except Exception:
            logger.exception("internal RAG retrieval failed user_id=%s", user_id)

    return "\n\n".join(blocks) if blocks else None


async def _event_stream(source, request: Request, payload: InternalAgentRequest):
    event_id = 0
    started = time.perf_counter()

    def event(name: str, data: dict[str, object]) -> str:
        nonlocal event_id
        event_id += 1
        return format_sse(name, data, event_id)

    try:
        async for item in source:
            if await request.is_disconnected():
                break
            if item is None:
                yield heartbeat()
                continue
            yield event(item.event, item.data)
    except Exception:
        logger.exception(
            "internal AI stream failed request_id=%s session_id=%s mode=%s",
            payload.request_id,
            payload.session_id,
            payload.mode,
        )
        yield event(
            "error",
            {
                "code": "INTERNAL_ERROR",
                "message": "AI 服务内部错误",
                "retryable": False,
                "run_id": payload.request_id,
            },
        )
    finally:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        logger.info(
            "internal AI stream finished request_id=%s session_id=%s "
            "mode=%s elapsed_ms=%s",
            payload.request_id,
            payload.session_id,
            payload.mode,
            elapsed_ms,
        )


def _now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


# Keep the import used by tests/type checkers that inspect public symbols.
__all__ = [
    "InternalAgentRequest",
    "ContextMessage",
    "router",
]