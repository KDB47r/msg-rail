from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.api.deps import get_current_user
from app.memory.store import MemoryRecord
from app.storage.models import User

router = APIRouter(prefix="/v1/memories", tags=["memories"])


def _iso(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _memory_dict(record: MemoryRecord) -> dict[str, object]:
    return {
        "id": record.id,
        "kind": record.kind,
        "content": record.content,
        "confidence": record.confidence,
        "importance": record.importance,
        "status": record.status,
        "created_at": _iso(record.created_at),
        "last_seen_at": _iso(record.last_seen_at),
    }


@router.get("")
async def list_memories(
    request: Request,
    user: User = Depends(get_current_user),
) -> dict[str, object]:
    if not request.app.state.settings.memory_enabled:
        return {"memories": []}

    records = await request.app.state.memory_store.list_for_user(user.id)
    return {"memories": [_memory_dict(record) for record in records]}


@router.delete("/{memory_id}")
async def delete_memory(
    memory_id: int,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict[str, str]:
    if not request.app.state.settings.memory_enabled:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="memory not found",
        )

    deleted = await request.app.state.memory_store.delete(
        user_id=user.id,
        memory_id=memory_id,
    )
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="memory not found",
        )
    return {"status": "deleted"}


@router.delete("")
async def delete_all_memories(
    request: Request,
    user: User = Depends(get_current_user),
) -> dict[str, int]:
    if not request.app.state.settings.memory_enabled:
        return {"deleted": 0}

    deleted = await request.app.state.memory_store.delete_all(user.id)
    return {"deleted": deleted}
