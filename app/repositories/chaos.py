import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.chaos import ChaosExperiment


class ChaosRepository:
    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    async def create(self, experiment: ChaosExperiment) -> ChaosExperiment:
        self.db.add(experiment)
        await self.db.flush()
        await self.db.refresh(experiment)
        return experiment

    async def get_by_id(self, experiment_id: uuid.UUID) -> ChaosExperiment | None:
        return await self.db.get(ChaosExperiment, experiment_id)

    async def list_by_tenant(
        self,
        tenant_id: uuid.UUID,
        cluster_id: uuid.UUID | None = None,
    ) -> list[ChaosExperiment]:
        stmt = select(ChaosExperiment).where(ChaosExperiment.tenant_id == tenant_id)

        if cluster_id:
            stmt = stmt.where(ChaosExperiment.cluster_id == cluster_id)

        stmt = stmt.order_by(ChaosExperiment.created_at.desc())
        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    async def update(
        self,
        experiment: ChaosExperiment,
        **kwargs: Any,  # noqa: ANN401
    ) -> ChaosExperiment:
        for key, value in kwargs.items():
            setattr(experiment, key, value)
        await self.db.flush()
        await self.db.refresh(experiment)
        return experiment
