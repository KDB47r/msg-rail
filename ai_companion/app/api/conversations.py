from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user, get_session
from app.storage.models import Conversation, Message, User
from app.storage.repositories import ConversationRepository, MessageRepository

router = APIRouter(prefix="/v1/conversations", tags=["conversations"])


class ConversationCreate(BaseModel):
    title: str | None = Field(default=None, max_length=200)
    persona_id: str = Field(default="default", max_length=64)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _conversation_dict(conversation: Conversation) -> dict[str, object]:
    return {
        "id": conversation.id,
        "title": conversation.title,
        "persona_id": conversation.persona_id,
        "status": conversation.status,
        "created_at": _iso(conversation.created_at),
        "updated_at": _iso(conversation.updated_at),
        "last_message_at": _iso(conversation.last_message_at),
    }


def _message_dict(message: Message) -> dict[str, object]:
    return {
        "id": message.id,
        "role": message.role,
        "content": message.content,
        "status": message.status,
        "token_count": message.token_count,
        "created_at": _iso(message.created_at),
    }


@router.get("")
async def list_conversations(
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> dict[str, object]:
    conversations = await ConversationRepository(session).list_for_user(user.id)
    return {"conversations": [_conversation_dict(item) for item in conversations]}


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_conversation(
    payload: ConversationCreate,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> dict[str, object]:
    conversation = await ConversationRepository(session).create(
        user_id=user.id,
        title=payload.title or "新对话",
        persona_id=payload.persona_id,
    )
    await session.commit()
    return _conversation_dict(conversation)


@router.get("/{conversation_id}/messages")
async def list_messages(
    conversation_id: str,
    limit: int = Query(default=50, ge=1, le=200),
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> dict[str, object]:
    conversation = await ConversationRepository(session).get(conversation_id)
    if conversation is None or conversation.user_id != user.id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="conversation not found",
        )

    messages = await MessageRepository(session).list_for_conversation(
        conversation_id,
        limit,
    )
    return {"messages": [_message_dict(item) for item in messages]}


@router.delete("/{conversation_id}")
async def archive_conversation(
    conversation_id: str,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> dict[str, str]:
    repository = ConversationRepository(session)
    conversation = await repository.get(conversation_id)
    if conversation is None or conversation.user_id != user.id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="conversation not found",
        )

    await repository.archive(conversation)
    await session.commit()
    return {"status": "archived"}
