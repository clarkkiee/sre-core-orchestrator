import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from fastapi import Query
from pydantic import BaseModel, Field

from app.models.chaos import ChaosExperimentStatus, ExperimentType
from app.schemas.pagination import DefaultDataPage


class StartChaosExperimentRequest(BaseModel):
    cluster_id: uuid.UUID
    deployment_id: uuid.UUID
    experiment_type: ExperimentType
    target_namespace: str
    target_label: str = Field(examples=["app=frontend"])
    duration_seconds: int = Field(default=60, ge=10, le=600)
    configuration: dict[str, Any] | None = None


class ChaosExperimentResponse(BaseModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    cluster_id: uuid.UUID
    deployment_id: uuid.UUID
    experiment_type: ExperimentType
    target_namespace: str
    target_label: str
    status: ChaosExperimentStatus
    status_message: str | None
    duration_seconds: int
    chaos_engine_name: str | None
    configuration: dict[str, Any] | None
    result: dict[str, Any] | None
    started_at: datetime | None
    completed_at: datetime | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


ChaosExperimentPage = DefaultDataPage[ChaosExperimentResponse]


class StopExperimentResponse(BaseModel):
    id: uuid.UUID
    status: ChaosExperimentStatus
    message: str


@dataclass(frozen=True)
class ChaosExperimentFilter:
    search: str | None = None
    status: ChaosExperimentStatus | None = None
    experiment_type: ExperimentType | None = None
    cluster_id: uuid.UUID | None = None
    campaign_id: uuid.UUID | None = None


class ChaosExperimentFilterParams:
    def __init__(
        self,
        search: str | None = Query(
            default=None,
            min_length=1,
            max_length=100,
            description="Free text search over target namespace / target label",
        ),
        status: ChaosExperimentStatus | None = Query(
            default=None,
            description="Filter by experiment status",
        ),
        experiment_type: ExperimentType | None = Query(
            default=None,
            description="Filter by experiment type",
        ),
        cluster_id: uuid.UUID | None = Query(
            default=None,
            description="Filter by cluster",
        ),
        campaign_id: uuid.UUID | None = Query(
            default=None,
            description="Filter by campaign"
        )
    ) -> None:
        self.search = search
        self.status = status
        self.experiment_type = experiment_type
        self.cluster_id = cluster_id
        self.campaign_id = campaign_id

    def to_filter(self) -> ChaosExperimentFilter:
        return ChaosExperimentFilter(
            search=self.search.strip() if self.search is not None else None,
            status=self.status,
            experiment_type=self.experiment_type,
            cluster_id=self.cluster_id,
            campaign_id=self.campaign_id,
        )
