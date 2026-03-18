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
    AdminDeploymentListResponse,
    AdminDeploymentResponse,
    CreateDeploymentRequest,
    DeleteDeploymentResponse,
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
    return await deployment_service.create_deployment(current_user, payload)


@router.get(
    "",
    response_model=DeploymentListResponse | AdminDeploymentListResponse,
    responses=error_responses(
        (UnauthorizedError, "Invalid or expired token"),
    ),
)
async def list_deployments(
    current_user: CurrentUser,
    deployment_service: DeploymentServiceDep,
) -> DeploymentListResponse | AdminDeploymentListResponse:
    """List deployments. Admins see all deployments; regular users see their own."""
    return await deployment_service.list_deployments(current_user)


@router.get(
    "/{deployment_id}",
    response_model=DeploymentResponse | AdminDeploymentResponse,
    responses=error_responses(
        (UnauthorizedError, "Invalid or expired token"),
        (NotFoundError, "Deployment not found"),
    ),
)
async def get_deployment(
    deployment_id: uuid.UUID,
    current_user: CurrentUser,
    deployment_service: DeploymentServiceDep,
) -> DeploymentResponse | AdminDeploymentResponse:
    """Get a specific deployment. Admins can access any deployment."""
    return await deployment_service.get_deployment(current_user, deployment_id)


@router.delete(
    "/{deployment_id}",
    response_model=DeleteDeploymentResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses=error_responses(
        (UnauthorizedError, "Invalid or expired token"),
        (NotFoundError, "Deployment not found"),
        (ConflictError, "Deployment is already being deleted"),
    ),
)
async def delete_deployment(
    deployment_id: uuid.UUID,
    current_user: CurrentUser,
    deployment_service: DeploymentServiceDep,
) -> DeleteDeploymentResponse:
    """Delete a deployment (async via Celery). Admins can delete any deployment."""
    return await deployment_service.delete_deployment(current_user, deployment_id)
