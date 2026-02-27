"""Deployment API endpoints."""

import uuid

from fastapi import APIRouter, status

from app.dependencies import CurrentUser, DeploymentServiceDep
from app.exceptions.errors import (
    ConflictError,
    NotFoundError,
    UnauthorizedError,
    error_responses,
)
from app.schemas.deployment import (
    CreateDeploymentRequest,
    DeploymentListResponse,
    DeploymentResponse,
    DeploymentWithJobResponse,
)

router = APIRouter(prefix="/deployments", tags=["deployments"])


@router.post(
    "",
    response_model=DeploymentWithJobResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses=error_responses(
        (UnauthorizedError, "Invalid or expired token"),
        (NotFoundError, "Cluster not found"),
        (ConflictError, "Cluster is not in READY status"),
    ),
)
async def create_deployment(
    payload: CreateDeploymentRequest,
    current_user: CurrentUser,
    deployment_service: DeploymentServiceDep,
) -> DeploymentWithJobResponse:
    """Deploy an application to a cluster (async via Celery)."""
    return await deployment_service.create_deployment(current_user.id, payload)


@router.get(
    "",
    response_model=DeploymentListResponse,
    responses=error_responses(
        (UnauthorizedError, "Invalid or expired token"),
    ),
)
async def list_deployments(
    current_user: CurrentUser,
    deployment_service: DeploymentServiceDep,
) -> DeploymentListResponse:
    """List all deployments for the current user."""
    return await deployment_service.list_deployments(current_user.id)


@router.get(
    "/{deployment_id}",
    response_model=DeploymentResponse,
    responses=error_responses(
        (UnauthorizedError, "Invalid or expired token"),
        (NotFoundError, "Deployment not found"),
    ),
)
async def get_deployment(
    deployment_id: uuid.UUID,
    current_user: CurrentUser,
    deployment_service: DeploymentServiceDep,
) -> DeploymentResponse:
    """Get a specific deployment's status and details."""
    return await deployment_service.get_deployment(current_user.id, deployment_id)
