from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.utils.config import settings


def _make_session_maker() -> async_sessionmaker[AsyncSession]:
    """Create a disposable async engine + session maker for a single task.

    Each call creates a fresh engine (with a small pool) so that forked
    Celery workers never share an event-loop-bound engine.  The engine
    is disposed automatically when the session context exits — see
    ``_task_session()``.
    """

    task_engine = create_async_engine(
        settings.DATABASE_URL,
        pool_size=2,
        max_overflow=3,
        pool_timeout=settings.POSTGRES_POOL_TIMEOUT,
        echo=settings.LOG_LEVEL == "DEBUG",
        future=True,
    )

    return async_sessionmaker(
        task_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
        autocommit=False,
    )


@asynccontextmanager
async def task_session() -> AsyncIterator[AsyncSession]:
    """Provide a session that automatically disposes its engine on exit.

    Usage::

        async with task_session() as session:
            repo = SomeRepository(session)
            ...
    """
    maker = _make_session_maker()
    engine = maker.kw["bind"]
    async with maker() as session:
        try:
            yield session
        finally:
            await engine.dispose()
