import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.campaign import ChaosCampaign


class CampaignRepository:
    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    async def create(self, campaign: ChaosCampaign) -> ChaosCampaign:
        self.db.add(campaign)
        await self.db.flush()
        await self.db.refresh(campaign)
        return campaign

    async def get_by_id(self, campaign_id: uuid.UUID) -> ChaosCampaign | None:
        stmt = (
            select(ChaosCampaign)
            .where(ChaosCampaign.id == campaign_id)
            .options(selectinload(ChaosCampaign.experiments))
        )
        result = await self.db.execute(stmt)
        return result.scalar_one_or_none()

    async def list_by_tenant(
        self,
        tenant_id: uuid.UUID,
        cluster_id: uuid.UUID | None = None,
    ) -> list[ChaosCampaign]:
        stmt = (
            select(ChaosCampaign)
            .where(ChaosCampaign.tenant_id == tenant_id)
            .options(selectinload(ChaosCampaign.experiments))
        )

        if cluster_id:
            stmt = stmt.where(ChaosCampaign.cluster_id == cluster_id)

        stmt = stmt.order_by(ChaosCampaign.created_at.desc())
        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    async def update(
        self,
        campaign: ChaosCampaign,
        **kwargs: Any,  # noqa: ANN401
    ) -> ChaosCampaign:
        for key, value in kwargs.items():
            setattr(campaign, key, value)
        await self.db.flush()
        await self.db.refresh(campaign)
        return campaign
