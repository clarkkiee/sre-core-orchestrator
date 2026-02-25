"""Pydantic schemas for cluster endpoints."""

import datetime
from typing import Any

from pydantic import BaseModel, Field


class CreateClusterRequest(BaseModel):
    name: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="User-provided cluster name",
    )
    app_preset: str | None = Field(
        None,
        max_length=100,
        description="Application preset",
    )
    worker_count: int = Field(
        default=2,
        ge=1,
        le=5,
        description="Number of worker nodes (1-5)",
    )
    expires_in_days: int = Field(
        default=7,
        ge=1,
        le=30,
        description="Days until cluster expires",
    )


class ClusterResponse(BaseModel):
    id: str
    name: str
    kind_name: str
    status: str
    status_message: str | None
    app_preset: str | None
    ports: dict[str, Any] | None
    has_kubeconfig: bool = False
    worker_count: int | None
    created_at: datetime.datetime
    updated_at: datetime.datetime
    expires_at: datetime.datetime | None


class ClusterListResponse(BaseModel):
    clusters: list[ClusterResponse]
    total: int


class ClusterWithJobResponse(ClusterResponse):
    job_id: str | None = None
    job_status: str | None = None


class DeleteClusterResponse(BaseModel):
    id: str
    status: str
    job_id: str
    message: str
