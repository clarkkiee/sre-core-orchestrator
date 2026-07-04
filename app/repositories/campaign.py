import uuid

from sqlalchemy import Select, select

from app.models.campaign import ChaosCampaign
from app.repositories.base import BaseRepository
from app.schemas.campaign import CampaignFilter
from app.schemas.pagination import Pagination


class CampaignRepository(BaseRepository[ChaosCampaign]):
    async def get_by_id(self, campaign_id: uuid.UUID) -> ChaosCampaign | None:
        stmt = select(ChaosCampaign).where(ChaosCampaign.id == campaign_id)
        result = await self.db.execute(stmt)
        return result.scalar_one_or_none()

    async def list_by_tenant(
        self,
        tenant_id: uuid.UUID,
        *,
        pagination: Pagination,
        filters: CampaignFilter | None = None,
    ) -> tuple[list[ChaosCampaign], int]:
        stmt = select(ChaosCampaign).where(ChaosCampaign.tenant_id == tenant_id)
        stmt = self._apply_filters(stmt, filters)
        stmt = stmt.order_by(ChaosCampaign.created_at.desc())
        return await self.paginate(stmt, pagination)

    async def list_all(
        self,
        *,
        pagination: Pagination,
        filters: CampaignFilter | None = None,
    ) -> tuple[list[ChaosCampaign], int]:
        stmt = select(ChaosCampaign)
        stmt = self._apply_filters(stmt, filters)
        stmt = stmt.order_by(ChaosCampaign.created_at.desc())
        return await self.paginate(stmt, pagination)

    @staticmethod
    def _apply_filters(
        stmt: Select[tuple[ChaosCampaign]],
        filters: CampaignFilter | None = None,
    ) -> Select[tuple[ChaosCampaign]]:
        if not filters:
            return stmt
        if filters.status is not None:
            stmt = stmt.where(ChaosCampaign.status == filters.status)
        if filters.cluster_id is not None:
            stmt = stmt.where(ChaosCampaign.cluster_id == filters.cluster_id)
        if filters.search:
            pattern = f"%{filters.search}%"
            stmt = stmt.where(ChaosCampaign.target_namespace.ilike(pattern))
        return stmt
