import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from app.models.chaos import ChaosExperimentStatus, ExperimentType


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


class ChaosExperimentListResponse(BaseModel):
    experiments: list[ChaosExperimentResponse]
    total: int


class StopExperimentResponse(BaseModel):
    id: uuid.UUID
    status: ChaosExperimentStatus
    message: str
