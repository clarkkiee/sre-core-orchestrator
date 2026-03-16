from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class StartCollectionRequest(BaseModel):
    cluster_id: str = Field(..., description="UUID of target cluster")

    deployment_id: str = Field(..., description="UUID of the deployment to observe")

    phase: str = Field(
        ...,
        description="Collection phase: BASELINE, CHAOS, RECOVERY",
        examples=["BASELINE"],
    )

    target_namespace: str = Field(
        default="default",
        max_length=255,
        description="Kubernetes namespace to collect metrics from",
    )

    collection_duration_seconds: int = Field(
        default=300,
        ge=60,
        le=3600,
        description="How long to collect metrics (seconds)",
    )

    collection_interval_seconds: int = Field(
        default=15,
        ge=5,
        le=60,
        description="Polling interval between metric snapshots (seconds)",
    )


class SessionResponse(BaseModel):
    id: str
    cluster_id: str
    deployment_id: str
    phase: str
    status: str
    status_message: str | None = None
    target_namespace: str
    collection_duration_seconds: int
    collection_interval_seconds: int
    started_at: datetime | None
    completed_at: datetime | None
    created_at: datetime
    updated_at: datetime


class SessionListResponse(BaseModel):
    sessions: list[SessionResponse]
    total: int


class SessionWithJobResponse(SessionResponse):
    job_id: str | None = None
    job_status: str | None = None


class MetricSnapshotResponse(BaseModel):
    sli_name: str
    display_name: str
    sub_characteristics: str
    value: float | None
    unit: str
    slo_target: float
    slo_operator: str
    slo_met: bool | None
    collected_at: datetime


class SessionMetricsResponse(BaseModel):
    session_id: str
    phase: str
    snapshots: list[MetricSnapshotResponse]
    total: int


class GenerateReportRequest(BaseModel):
    cluster_id: str = Field(..., description="UUID of the cluster")
    deployment_id: str = Field(..., description="UUID of the deployment")
    baseline_session_id: str = Field(..., description="UUID of the baseline session")
    chaos_session_id: str = Field(..., description="UUID of the chaos session")
    recovery_session_id: str = Field(..., description="UUID of the recovery session")


class ReliabilityReportResponse(BaseModel):
    id: str
    cluster_id: str
    deployment_id: str
    baseline_session_id: str
    chaos_session_id: str
    recovery_session_id: str
    overall_score: float
    availability_score: float
    faultlessness_score: float
    fault_tolerance_score: float
    recoverability_score: float
    details: dict[str, Any] | None
    created_at: datetime


class SLIDefinitionResponse(BaseModel):
    id: str
    name: str
    sub_characteristics: str
    promql_template: str
    unit: str
    slo_target: float
    slo_operator: str
    description: str
