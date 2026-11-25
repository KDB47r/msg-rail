from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.pool import StaticPool


class Base(DeclarativeBase):
    pass


def build_engine(database_url: str, echo: bool = False) -> AsyncEngine:
    kwargs: dict[str, object] = {
        "echo": echo,
        "pool_pre_ping": True,
    }

    # In-memory SQLite is used by tests; every connection must share the same DB.
    if database_url.startswith("sqlite"):
        kwargs["poolclass"] = StaticPool
        kwargs["connect_args"] = {"check_same_thread": False}

    return create_async_engine(database_url, **kwargs)


def build_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
