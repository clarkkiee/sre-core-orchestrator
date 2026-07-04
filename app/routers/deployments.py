"""Deployment API endpoints."""

import uuid

from fastapi import APIRouter, Depends, status

from app.dependencies import CurrentUser, DeploymentServiceDep
from app.exceptions.errors import (
    ConflictError,
    NotFoundError,
    UnauthorizedError,
    error_responses,
)
from app.schemas.deployment import (
    AdminDeploymentPage,
    AdminDeploymentResponse,
    CreateDeploymentRequest,
    DeleteDeploymentResponse,
    DeploymentFilterParams,
    DeploymentPage,
    DeploymentResponse,
    DeploymentWithJobResponse,
)
from app.schemas.pagination import PaginationParams

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
    response_model=DeploymentPage | AdminDeploymentPage,
    responses=error_responses(
        (UnauthorizedError, "Invalid or expired token"),
    ),
)
async def list_deployments(
    current_user: CurrentUser,
    deployment_service: DeploymentServiceDep,
    params: PaginationParams = Depends(),
    filters: DeploymentFilterParams = Depends(),
) -> DeploymentPage | AdminDeploymentPage:
    """List deployments. Admins see all deployments; regular users see their own."""
    return await deployment_service.list_deployments(
        current_user, params, filters.to_filter()
    )


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
