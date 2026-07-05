import uuid

from sqlalchemy import Select, or_, select, func

from app.models.chaos import ChaosExperiment
from app.repositories.base import BaseRepository
from app.schemas.chaos import ChaosExperimentFilter, ChaosExperimentStatus
from app.schemas.pagination import Pagination


class ChaosRepository(BaseRepository[ChaosExperiment]):
    async def get_by_id(self, experiment_id: uuid.UUID) -> ChaosExperiment | None:
        return await self.db.get(ChaosExperiment, experiment_id)

    async def list_by_tenant(
        self,
        tenant_id: uuid.UUID,
        *,
        pagination: Pagination,
        filters: ChaosExperimentFilter | None = None,
    ) -> tuple[list[ChaosExperiment], int]:
        stmt = select(ChaosExperiment).where(ChaosExperiment.tenant_id == tenant_id)
        stmt = self._apply_filters(stmt, filters)
        stmt = stmt.order_by(ChaosExperiment.created_at.desc())
        return await self.paginate(stmt, pagination)

    async def list_all(
        self,
        *,
        pagination: Pagination,
        filters: ChaosExperimentFilter | None = None,
    ) -> tuple[list[ChaosExperiment], int]:
        stmt = select(ChaosExperiment)
        stmt = self._apply_filters(stmt, filters)
        stmt = stmt.order_by(ChaosExperiment.created_at.desc())
        return await self.paginate(stmt, pagination)
    
    async def count_by_status(
        self,
        tenant_id: uuid.UUID | None = None
    ) -> dict[ChaosExperimentStatus, int]:
        stmt = select(ChaosExperiment.status, func.count()).group_by(ChaosExperiment.status)
        if tenant_id is not None:
            stmt = stmt.where(ChaosExperiment.tenant_id == tenant_id)
        result = await self.db.execute(stmt)
        counts = {status: count for status, count in result.all()}
        return {
            status: counts.get(status, 0) for status in ChaosExperimentStatus
        }

    @staticmethod
    def _apply_filters(
        stmt: Select[tuple[ChaosExperiment]],
        filters: ChaosExperimentFilter | None = None,
    ) -> Select[tuple[ChaosExperiment]]:
        if not filters:
            return stmt
        if filters.status is not None:
            stmt = stmt.where(ChaosExperiment.status == filters.status)
        if filters.experiment_type is not None:
            stmt = stmt.where(
                ChaosExperiment.experiment_type == filters.experiment_type
            )
        if filters.cluster_id is not None:
            stmt = stmt.where(ChaosExperiment.cluster_id == filters.cluster_id)
        if filters.campaign_id is not None:
            stmt = stmt.where(ChaosExperiment.campaign_id == filters.campaign_id)
        if filters.search:
            pattern = f"%{filters.search}%"
            stmt = stmt.where(
                or_(
                    ChaosExperiment.target_namespace.ilike(pattern),
                    ChaosExperiment.target_label.ilike(pattern),
                )
            )
        return stmt
