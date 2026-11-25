from datetime import datetime
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.storage.models import Conversation, Message, Run, Summary, User, utcnow


class UserRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_or_create(
        self,
        external_id: str,
        display_name: str = "Dev User",
    ) -> User:
        user = await self._session.scalar(
            select(User).where(User.external_id == external_id)
        )
        if user is not None:
            return user

        user = User(external_id=external_id, display_name=display_name)
        self._session.add(user)
        await self._session.flush()
        return user


class ConversationRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, conversation_id: str) -> Conversation | None:
        return await self._session.get(Conversation, conversation_id)

    async def create(
        self,
        *,
        user_id: int,
        title: str = "新对话",
        persona_id: str = "default",
    ) -> Conversation:
        conversation = Conversation(
            id=f"c_{uuid4().hex}",
            user_id=user_id,
            title=title,
            persona_id=persona_id,
            status="active",
        )
        self._session.add(conversation)
        await self._session.flush()
        return conversation

    async def list_for_user(self, user_id: int, limit: int = 50) -> list[Conversation]:
        result = await self._session.scalars(
            select(Conversation)
            .where(
                Conversation.user_id == user_id,
                Conversation.status == "active",
            )
            .order_by(Conversation.created_at.desc())
            .limit(limit)
        )
        return list(result.all())

    async def touch(
        self,
        conversation: Conversation,
        *,
        first_user_message: str | None = None,
    ) -> None:
        now = utcnow()
        conversation.last_message_at = now
        conversation.updated_at = now

        if first_user_message and conversation.title in (None, "", "新对话"):
            conversation.title = first_user_message[:30]

    async def archive(self, conversation: Conversation) -> None:
        conversation.status = "archived"
        conversation.updated_at = utcnow()


class MessageRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, message_id: str) -> Message | None:
        return await self._session.get(Message, message_id)

    async def create(
        self,
        *,
        conversation_id: str,
        role: str,
        content: str,
        status: str = "completed",
        client_message_id: str | None = None,
        token_count: int | None = None,
        model: str | None = None,
        provider: str | None = None,
        message_id: str | None = None,
    ) -> Message:
        message = Message(
            id=message_id or f"m_{uuid4().hex}",
            conversation_id=conversation_id,
            role=role,
            content=content,
            status=status,
            client_message_id=client_message_id,
            token_count=token_count,
            model=model,
            provider=provider,
        )
        self._session.add(message)
        await self._session.flush()
        return message

    async def upsert_assistant(
        self,
        *,
        message_id: str,
        conversation_id: str,
        content: str,
        status: str,
        token_count: int | None,
        model: str | None,
        provider: str | None,
    ) -> Message:
        message = await self._session.get(Message, message_id)
        if message is None:
            message = Message(
                id=message_id,
                conversation_id=conversation_id,
                role="assistant",
                content=content,
                status=status,
                token_count=token_count,
                model=model,
                provider=provider,
            )
            self._session.add(message)
        else:
            message.content = content
            message.status = status
            message.token_count = token_count
            message.model = model
            message.provider = provider

        await self._session.flush()
        return message

    async def find_by_client_message_id(
        self,
        conversation_id: str,
        client_message_id: str,
    ) -> Message | None:
        return await self._session.scalar(
            select(Message)
            .where(
                Message.conversation_id == conversation_id,
                Message.client_message_id == client_message_id,
            )
            .limit(1)
        )

    async def list_completed_after(
        self,
        conversation_id: str,
        after_created_at: datetime | None,
    ) -> list[Message]:
        stmt = select(Message).where(
            Message.conversation_id == conversation_id,
            Message.role.in_(("user", "assistant")),
            Message.status == "completed",
        )
        if after_created_at is not None:
            stmt = stmt.where(Message.created_at > after_created_at)

        result = await self._session.scalars(
            stmt.order_by(Message.created_at.asc())
        )
        return list(result.all())

    async def list_for_conversation(
        self,
        conversation_id: str,
        limit: int = 50,
    ) -> list[Message]:
        result = await self._session.scalars(
            select(Message)
            .where(
                Message.conversation_id == conversation_id,
                Message.role.in_(("user", "assistant")),
            )
            .order_by(Message.created_at.desc())
            .limit(limit)
        )
        return list(reversed(result.all()))


class RunRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, run_id: str) -> Run | None:
        return await self._session.get(Run, run_id)

    async def create(
        self,
        *,
        conversation_id: str,
        user_message_id: str,
        model: str,
        provider: str,
        prompt_version: str,
    ) -> Run:
        run = Run(
            id=f"r_{uuid4().hex}",
            conversation_id=conversation_id,
            user_message_id=user_message_id,
            status="running",
            model=model,
            provider=provider,
            prompt_version=prompt_version,
        )
        self._session.add(run)
        await self._session.flush()
        return run

    async def find_by_user_message_id(self, user_message_id: str) -> Run | None:
        return await self._session.scalar(
            select(Run)
            .where(Run.user_message_id == user_message_id)
            .order_by(Run.created_at.desc())
            .limit(1)
        )


class SummaryRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def latest(self, conversation_id: str) -> Summary | None:
        return await self._session.scalar(
            select(Summary)
            .where(Summary.conversation_id == conversation_id)
            .order_by(Summary.created_at.desc())
            .limit(1)
        )

    async def create(
        self,
        *,
        conversation_id: str,
        up_to_message_id: str,
        up_to_created_at: datetime,
        content: str,
        token_count: int | None,
    ) -> Summary:
        summary = Summary(
            conversation_id=conversation_id,
            up_to_message_id=up_to_message_id,
            up_to_created_at=up_to_created_at,
            content=content,
            token_count=token_count,
        )
        self._session.add(summary)
        await self._session.flush()
        return summary
