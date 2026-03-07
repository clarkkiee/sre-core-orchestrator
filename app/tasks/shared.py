from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.utils.config import settings


def _make_session_maker() -> async_sessionmaker[AsyncSession]:
    """Create a fresh async engine + session maker for each task invocation.

    This avoids the 'Future attached to a different loop' error that occurs
    when a module-level engine is reused across multiple ``asyncio.run()``
    calls in Celery's forked worker processes.
    """

    task_engine = create_async_engine(
        settings.DATABASE_URL,
        pool_size=settings.POSTGRES_POOL_SIZE,
        max_overflow=settings.POSTGRES_MAX_OVERFLOW,
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
