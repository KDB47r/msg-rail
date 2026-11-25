import logging
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.api.deps import get_current_user
from app.rag.service import TooManyChunks
from app.storage.models import User

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/documents", tags=["documents"])


def _iso(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _validate_title(title: str) -> str:
    name = Path(title).name.strip() or "untitled.md"
    lower = name.lower()
    if not (lower.endswith(".txt") or lower.endswith(".md")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="only .txt and .md files are supported",
        )
    return name


@router.post("", status_code=status.HTTP_201_CREATED)
async def upload_document(
    request: Request,
    user: User = Depends(get_current_user),
) -> dict[str, object]:
    settings = request.app.state.settings
    max_bytes = settings.rag_max_file_bytes

    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > max_bytes:
        raise HTTPException(
            status_code=status.HTTP_413_CONTENT_TOO_LARGE,
            detail="file is too large",
        )

    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        try:
            payload = await request.json()
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="invalid JSON body",
            ) from exc
        if not isinstance(payload, dict):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="invalid JSON body",
            )
        title = _validate_title(str(payload.get("title") or "untitled.md"))
        content = str(payload.get("content") or "")
    else:
        title = _validate_title(
            request.headers.get("x-filename") or "untitled.md"
        )
        try:
            content = (await request.body()).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="file must be UTF-8 text",
            ) from exc

    if len(content.encode("utf-8")) > max_bytes:
        raise HTTPException(
            status_code=status.HTTP_413_CONTENT_TOO_LARGE,
            detail="file is too large",
        )
    if not content.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="document is empty",
        )

    try:
        result = await request.app.state.rag_service.ingest(
            user_id=user.id,
            title=title,
            source="upload",
            content=content,
        )
    except TooManyChunks as exc:
        raise HTTPException(
            status_code=status.HTTP_413_CONTENT_TOO_LARGE,
            detail=str(exc),
        ) from exc
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except Exception as exc:
        logger.exception("document ingestion failed user_id=%s", user.id)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="document ingestion unavailable",
        ) from exc

    return result


@router.get("")
async def list_documents(
    request: Request,
    user: User = Depends(get_current_user),
) -> dict[str, object]:
    documents = await request.app.state.rag_service.list_documents(user.id)
    return {
        "documents": [
            {
                "id": document.id,
                "title": document.title,
                "source": document.source,
                "chunk_count": document.chunk_count,
                "created_at": _iso(document.created_at),
            }
            for document in documents
        ]
    }


@router.delete("/{document_id}")
async def delete_document(
    document_id: int,
    request: Request,
    user: User = Depends(get_current_user),
) -> dict[str, str]:
    deleted = await request.app.state.rag_service.delete(
        user.id,
        document_id,
    )
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="document not found",
        )
    return {"status": "deleted"}
