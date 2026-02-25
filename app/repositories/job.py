"""Job repository — database operations for job tracking."""

import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.job import Job


class JobRepository:
    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    async def create(self, job: Job) -> Job:
        self.db.add(job)
        await self.db.flush()
        await self.db.refresh(job)
        return job

    async def get_by_id(self, job_id: uuid.UUID) -> Job | None:
        result = await self.db.execute(select(Job).where(Job.id == job_id))
        return result.scalar_one_or_none()

    async def get_by_celery_task_id(self, celery_task_id: str) -> Job | None:
        result = await self.db.execute(
            select(Job).where(Job.celery_task_id == celery_task_id),
        )
        return result.scalar_one_or_none()

    async def update(self, job: Job, **fields: object) -> Job:
        for key, value in fields.items():
            setattr(job, key, value)
        await self.db.flush()
        await self.db.refresh(job)
        return job

    async def list_by_cluster(self, cluster_id: uuid.UUID) -> list[Job]:
        stmt = (
            select(Job)
            .where(Job.cluster_id == cluster_id)
            .order_by(Job.created_at.desc())
        )
        result = await self.db.execute(stmt)
        return list(result.scalars().all())
