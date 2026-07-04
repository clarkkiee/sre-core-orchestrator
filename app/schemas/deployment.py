"""Pydantic schemas for deployment endpoints."""

import datetime
from dataclasses import dataclass
from typing import Any

from fastapi import Query
from pydantic import BaseModel, Field

from app.models.deployment import DeploymentStatus, DeployStrategy
from app.schemas.pagination import DefaultDataPage


class CreateDeploymentRequest(BaseModel):
    repo_url: str = Field(
        ...,
        min_length=1,
        max_length=500,
        description="GitHub repository URL",
        examples=["https://github.com/GoogleCloudPlatform/microservices-demo"],
    )
    branch: str | None = Field(
        None,
        max_length=255,
        description="Git branch override (defaults to .platform.yaml source.branch)",
    )
    cluster_id: str = Field(
        ...,
        description="UUID of the target cluster (must be in READY status)",
    )
    namespace: str = Field(
        default="default",
        max_length=255,
        description="Kubernetes namespace to deploy into",
    )
    github_token: str | None = Field(
        None,
        description="GitHub PAT for private repositories (not persisted)",
    )


class DeploymentResponse(BaseModel):
    id: str
    cluster_id: str
    repo_url: str
    branch: str
    strategy: str
    namespace: str
    status: str
    status_message: str | None
    platform_config: dict[str, Any] | None
    created_at: datetime.datetime
    updated_at: datetime.datetime
    completed_at: datetime.datetime | None
    deleted_at: datetime.datetime | None = None


class DeleteDeploymentResponse(BaseModel):
    id: str
    status: str
    job_id: str
    message: str


class DeploymentWithJobResponse(DeploymentResponse):
    job_id: str | None = None
    job_status: str | None = None


class AdminDeploymentResponse(DeploymentResponse):
    tenant_id: str


@dataclass(frozen=True)
class DeploymentFilter:
    search: str | None = None
    status: DeploymentStatus | None = None
    strategy: DeployStrategy | None = None


class DeploymentFilterParams:
    def __init__(
        self,
        search: str | None = Query(
            default=None,
            min_length=1,
            max_length=100,
            description="Free text search over repo URL / namespace / branch",
        ),
        status: DeploymentStatus | None = Query(
            default=None,
            description="Filter by deployment status",
        ),
        strategy: DeployStrategy | None = Query(
            default=None,
            description="Filter by deployment strategy",
        ),
    ) -> None:
        self.search = search
        self.status = status
        self.strategy = strategy

    def to_filter(self) -> DeploymentFilter:
        return DeploymentFilter(
            search=self.search.strip() if self.search is not None else None,
            status=self.status,
            strategy=self.strategy,
        )


DeploymentPage = DefaultDataPage[DeploymentResponse]
AdminDeploymentPage = DefaultDataPage[AdminDeploymentResponse]
