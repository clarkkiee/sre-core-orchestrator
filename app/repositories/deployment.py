"""Deployment repository -- database operations for deployment management."""

import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.deployment import Deployment


class DeploymentRepository:
    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    async def create(self, deployment: Deployment) -> Deployment:
        self.db.add(deployment)
        await self.db.flush()
        await self.db.refresh(deployment)
        return deployment

    async def get_by_id(self, deployment_id: uuid.UUID) -> Deployment | None:
        result = await self.db.execute(
            select(Deployment).where(Deployment.id == deployment_id),
        )
        return result.scalar_one_or_none()

    async def list_by_tenant(
        self,
        tenant_id: uuid.UUID,
    ) -> list[Deployment]:
        stmt = (
            select(Deployment)
            .where(Deployment.tenant_id == tenant_id)
            .where(Deployment.deleted_at.is_(None))
            .order_by(Deployment.created_at.desc())
        )
        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    async def list_by_cluster(
        self,
        cluster_id: uuid.UUID,
    ) -> list[Deployment]:
        stmt = (
            select(Deployment)
            .where(Deployment.cluster_id == cluster_id)
            .where(Deployment.deleted_at.is_(None))
            .order_by(Deployment.created_at.desc())
        )
        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    async def list_all(self) -> list[Deployment]:
        """Return all deployments across all tenants (excludes soft-deleted)."""
        stmt = (
            select(Deployment)
            .where(Deployment.deleted_at.is_(None))
            .order_by(Deployment.created_at.desc())
        )
        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    async def update(self, deployment: Deployment, **fields: object) -> Deployment:
        for key, value in fields.items():
            setattr(deployment, key, value)
        await self.db.flush()
        await self.db.refresh(deployment)
        return deployment
