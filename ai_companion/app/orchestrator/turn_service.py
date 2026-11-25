import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.llm.base import Message, Usage
from app.orchestrator.context_builder import PROMPT_VERSION, build_context
from app.storage.models import Run, utcnow
from app.storage.repositories import (
    ConversationRepository,
    MessageRepository,
    RunRepository,
    SummaryRepository,
)

logger = logging.getLogger(__name__)


class ConversationNotFound(Exception):
    """Conversation does not exist, is archived, or belongs to another user."""


class TurnInProgress(Exception):
    """The same client message is already being processed."""


@dataclass(slots=True)
class PreparedTurn:
    conversation_id: str
    user_id: int
    user_message_id: str
    assistant_message_id: str
    run_id: str
    created_at: str
    messages: list[Message]
    history_count: int
    needs_summary: bool


@dataclass(slots=True)
class ReplayTurn:
    conversation_id: str
    assistant_message_id: str
    run_id: str
    created_at: str
    content: str
    usage: Usage | None
    status: str


class TurnService:
    """Loads/saves one conversation turn around the streaming orchestrator."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
    ) -> None:
        self._session_factory = session_factory
        self._settings = settings

    async def prepare(
        self,
        *,
        user_id: int,
        conversation_id: str | None,
        message: str,
        client_message_id: str | None,
        persona_id: str,
        tool_spec_tokens: int = 0,
        memory_text: str | None = None,
        rag_text: str | None = None,
    ) -> PreparedTurn | ReplayTurn:
        async with self._session_factory() as session:
            conversations = ConversationRepository(session)
            messages = MessageRepository(session)
            runs = RunRepository(session)

            conversation = None
            if conversation_id:
                conversation = await conversations.get(conversation_id)
                if (
                    conversation is None
                    or conversation.user_id != user_id
                    or conversation.status != "active"
                ):
                    raise ConversationNotFound(conversation_id)
            else:
                conversation = await conversations.create(
                    user_id=user_id,
                    persona_id=persona_id,
                )

            if client_message_id:
                existing = await messages.find_by_client_message_id(
                    conversation.id,
                    client_message_id,
                )
                if existing is not None:
                    run = await runs.find_by_user_message_id(existing.id)
                    if (
                        run is not None
                        and run.status == "completed"
                        and run.assistant_message_id
                    ):
                        assistant = await messages.get(run.assistant_message_id)
                        if assistant is not None:
                            return ReplayTurn(
                                conversation_id=conversation.id,
                                assistant_message_id=assistant.id,
                                run_id=run.id,
                                created_at=assistant.created_at.isoformat(),
                                content=assistant.content,
                                usage=self._usage_from_run(run),
                                status=run.status,
                            )
                    raise TurnInProgress(conversation.id)

            summaries = SummaryRepository(session)
            latest_summary = await summaries.latest(conversation.id)
            history_rows = await messages.list_completed_after(
                conversation.id,
                latest_summary.up_to_created_at
                if latest_summary is not None
                else None,
            )
            history = [
                Message(role=row.role, content=row.content)
                for row in history_rows
                if row.role in ("user", "assistant") and row.content
            ]
            build = build_context(
                history,
                latest_summary.content if latest_summary is not None else None,
                message,
                context_window_tokens=self._settings.context_window_tokens,
                output_reserve_tokens=self._settings.llm_max_tokens,
                safety_margin=self._settings.token_safety_margin,
                summary_trigger_ratio=self._settings.summary_trigger_ratio,
                tool_spec_tokens=tool_spec_tokens,
                memory=memory_text,
                rag=rag_text,
            )

            user_row = await messages.create(
                conversation_id=conversation.id,
                role="user",
                content=message,
                status="completed",
                client_message_id=client_message_id,
            )
            run = await runs.create(
                conversation_id=conversation.id,
                user_message_id=user_row.id,
                model=self._settings.llm_model,
                provider=self._settings.llm_provider,
                prompt_version=PROMPT_VERSION,
            )
            await conversations.touch(
                conversation,
                first_user_message=message,
            )

            try:
                await session.commit()
            except IntegrityError as exc:
                await session.rollback()
                raise TurnInProgress(conversation.id) from exc

            return PreparedTurn(
                conversation_id=conversation.id,
                user_id=user_id,
                user_message_id=user_row.id,
                assistant_message_id=f"m_{uuid4().hex}",
                run_id=run.id,
                created_at=datetime.now(timezone.utc).isoformat(),
                messages=build.messages,
                history_count=len(history),
                needs_summary=build.needs_summary,
            )

    async def finish(
        self,
        *,
        prepared: PreparedTurn,
        content: str,
        status: str,
        usage: dict[str, object] | None,
        ttft_ms: int | None,
        latency_ms: int,
        error_code: str | None,
    ) -> None:
        async with self._session_factory() as session:
            messages = MessageRepository(session)
            run = await session.get(Run, prepared.run_id)
            if run is None:
                logger.warning(
                    "run not found while finishing run_id=%s",
                    prepared.run_id,
                )
                return

            output_tokens = _int_or_none(usage, "output_tokens")
            assistant = None
            if content or status == "completed":
                assistant = await messages.upsert_assistant(
                    message_id=prepared.assistant_message_id,
                    conversation_id=prepared.conversation_id,
                    content=content,
                    status=status,
                    token_count=output_tokens,
                    model=self._settings.llm_model,
                    provider=self._settings.llm_provider,
                )

            run.status = status
            run.assistant_message_id = assistant.id if assistant is not None else None
            run.input_tokens = _int_or_none(usage, "input_tokens")
            run.output_tokens = output_tokens
            run.cached_tokens = _int_or_none(usage, "cached_tokens")
            run.ttft_ms = ttft_ms
            run.latency_ms = latency_ms
            run.error_code = error_code
            run.finished_at = utcnow()
            await session.commit()

    @staticmethod
    def _usage_from_run(run: Run) -> Usage | None:
        if run.input_tokens is None and run.output_tokens is None:
            return None
        return Usage(
            input_tokens=run.input_tokens or 0,
            output_tokens=run.output_tokens or 0,
            cached_tokens=run.cached_tokens or 0,
        )


def _int_or_none(usage: dict[str, object] | None, key: str) -> int | None:
    if usage is None:
        return None
    value = usage.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return int(value)
