import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel

from app.models.campaign import CampaignStatus
from app.schemas.chaos import ChaosExperimentResponse


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
    experiments: list[ChaosExperimentResponse]
    started_at: datetime | None
    completed_at: datetime | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class CampaignListResponse(BaseModel):
    campaigns: list[CampaignResponse]
    total: int


class StopCampaignResponse(BaseModel):
    id: uuid.UUID
    status: CampaignStatus
    message: str
