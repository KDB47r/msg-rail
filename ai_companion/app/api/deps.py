import logging
from collections.abc import AsyncIterator

from fastapi import Depends, Header, HTTPException, Request, status
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.security.auth import check_bearer_token
from app.storage.models import User
from app.storage.repositories import UserRepository

logger = logging.getLogger(__name__)


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.session_factory() as session:
        yield session


async def get_current_user(
    request: Request,
    session: AsyncSession = Depends(get_session),
    authorization: str | None = Header(default=None),
) -> User:
    settings = request.app.state.settings
    check_bearer_token(authorization, settings.dev_token)

    repo = UserRepository(session)
    try:
        try:
            user = await repo.get_or_create(settings.dev_user_external_id)
            await session.commit()
        except IntegrityError:
            # Two first requests may race to create the dev user.
            await session.rollback()
            user = await repo.get_or_create(settings.dev_user_external_id)
            await session.commit()
    except SQLAlchemyError as exc:
        await session.rollback()
        logger.exception("failed to resolve current user")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="database unavailable",
        ) from exc

    return user
