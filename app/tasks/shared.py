from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from app.models.job import Job
from app.repositories.job import JobRepository

from app.utils.config import settings


def _make_session_maker() -> async_sessionmaker[AsyncSession]:
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
    maker = _make_session_maker()
    engine = maker.kw["bind"]
    async with maker() as session:
        try:
            yield session
        finally:
            await engine.dispose()

class JobProgress:
    def __init__(self, job_repo: JobRepository, job: Job) -> None:
        self._repo = job_repo
        self._job = job

    async def __call__(self, phase: str, pct: int) -> None:
        await self._repo.update(
            self._job, current_phase=phase, progress_percentage=pct
        )
        await self._repo.db.commit()
