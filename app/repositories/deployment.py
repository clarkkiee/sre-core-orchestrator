"""Deployment repository -- database operations for deployment management."""

import uuid

from sqlalchemy import Select, or_, select, func

from app.models.deployment import Deployment
from app.repositories.base import BaseRepository
from app.schemas.deployment import DeploymentFilter, DeploymentStatus
from app.schemas.pagination import Pagination


class DeploymentRepository(BaseRepository[Deployment]):
    async def get_by_id(self, deployment_id: uuid.UUID) -> Deployment | None:
        result = await self.db.execute(
            select(Deployment).where(Deployment.id == deployment_id),
        )
        return result.scalar_one_or_none()

    async def list_by_tenant(
        self,
        tenant_id: uuid.UUID,
        *,
        pagination: Pagination,
        filters: DeploymentFilter | None = None,
    ) -> tuple[list[Deployment], int]:
        stmt = (
            select(Deployment)
            .where(Deployment.tenant_id == tenant_id)
            .where(Deployment.deleted_at.is_(None))
        )
        stmt = self._apply_filters(stmt, filters)
        stmt = stmt.order_by(Deployment.created_at.desc())
        return await self.paginate(stmt, pagination)

    async def list_all(
        self,
        *,
        pagination: Pagination,
        filters: DeploymentFilter | None = None,
    ) -> tuple[list[Deployment], int]:
        """Return all deployments across all tenants (excludes soft-deleted)."""
        stmt = select(Deployment).where(Deployment.deleted_at.is_(None))
        stmt = self._apply_filters(stmt, filters)
        stmt = stmt.order_by(Deployment.created_at.desc())
        return await self.paginate(stmt, pagination)

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
    
    async def count_by_status(
        self,
        tenant_id: uuid.UUID | None = None
    ) -> dict[DeploymentStatus, int]:
        stmt = select(Deployment.status, func.count()).group_by(Deployment.status)
        if tenant_id is not None:
            stmt = stmt.where(Deployment.tenant_id == tenant_id)
        result = await self.db.execute(stmt)
        counts = {status: count for status, count in result.all()}
        return {
            status: counts.get(status, 0) for status in DeploymentStatus
        }

    @staticmethod
    def _apply_filters(
        stmt: Select[tuple[Deployment]],
        filters: DeploymentFilter | None = None,
    ) -> Select[tuple[Deployment]]:
        if not filters:
            return stmt
        if filters.status is not None:
            stmt = stmt.where(Deployment.status == filters.status)
        if filters.strategy is not None:
            stmt = stmt.where(Deployment.strategy == filters.strategy)
        if filters.search:
            pattern = f"%{filters.search}%"
            stmt = stmt.where(
                or_(
                    Deployment.repo_url.ilike(pattern),
                    Deployment.namespace.ilike(pattern),
                    Deployment.branch.ilike(pattern),
                )
            )
        return stmt
