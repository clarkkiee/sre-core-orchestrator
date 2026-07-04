import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from fastapi import Query
from pydantic import BaseModel

from app.models.campaign import CampaignStatus
from app.schemas.pagination import DefaultDataPage


class StartCampaignRequest(BaseModel):
    cluster_id: uuid.UUID
    deployment_id: uuid.UUID


class CampaignResponse(BaseModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    cluster_id: uuid.UUID
    deployment_id: uuid.UUID
    target_namespace: str
    status: CampaignStatus
    status_message: str | None
    discovered_services: list[dict[str, Any]] | None
    total_experiments: int
    completed_experiments: int
    started_at: datetime | None
    completed_at: datetime | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


CampaignPage = DefaultDataPage[CampaignResponse]


class StopCampaignResponse(BaseModel):
    id: uuid.UUID
    status: CampaignStatus
    message: str


@dataclass(frozen=True)
class CampaignFilter:
    search: str | None = None
    status: CampaignStatus | None = None
    cluster_id: uuid.UUID | None = None


class CampaignFilterParams:
    def __init__(
        self,
        search: str | None = Query(
            default=None,
            min_length=1,
            max_length=100,
            description="Free text search over target namespace",
        ),
        status: CampaignStatus | None = Query(
            default=None,
            description="Filter by campaign status",
        ),
        cluster_id: uuid.UUID | None = Query(
            default=None,
            description="Filter by cluster",
        ),
    ) -> None:
        self.search = search
        self.status = status
        self.cluster_id = cluster_id

    def to_filter(self) -> CampaignFilter:
        return CampaignFilter(
            search=self.search.strip() if self.search is not None else None,
            status=self.status,
            cluster_id=self.cluster_id,
        )
