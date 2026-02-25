"""Cluster API endpoints."""

import uuid

from fastapi import APIRouter, status

from app.dependencies import ClusterServiceDep, CurrentUser
from app.exceptions.errors import (
    ConflictError,
    NotFoundError,
    UnauthorizedError,
    error_responses,
)
from app.schemas.cluster import (
    ClusterListResponse,
    ClusterResponse,
    ClusterWithJobResponse,
    CreateClusterRequest,
    DeleteClusterResponse,
)

router = APIRouter(prefix="/clusters", tags=["clusters"])


@router.post(
    "",
    response_model=ClusterWithJobResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses=error_responses(
        (UnauthorizedError, "Invalid or expired token"),
        (ConflictError, "Cluster with this name already exists"),
    ),
)
async def create_cluster(
    payload: CreateClusterRequest,
    current_user: CurrentUser,
    cluster_service: ClusterServiceDep,
) -> ClusterWithJobResponse:
    """Create a new KinD cluster (async via Celery)."""
    return await cluster_service.create_cluster(current_user.id, payload)


@router.get(
    "",
    response_model=ClusterListResponse,
    responses=error_responses(
        (UnauthorizedError, "Invalid or expired token"),
    ),
)
async def list_clusters(
    current_user: CurrentUser,
    cluster_service: ClusterServiceDep,
) -> ClusterListResponse:
    """List all clusters for the current user."""
    return await cluster_service.list_clusters(current_user.id)


@router.get(
    "/{cluster_id}",
    response_model=ClusterResponse,
    responses=error_responses(
        (UnauthorizedError, "Invalid or expired token"),
        (NotFoundError, "Cluster not found"),
    ),
)
async def get_cluster(
    cluster_id: uuid.UUID,
    current_user: CurrentUser,
    cluster_service: ClusterServiceDep,
) -> ClusterResponse:
    """Get a specific cluster's status and details."""
    return await cluster_service.get_cluster(current_user.id, cluster_id)


@router.delete(
    "/{cluster_id}",
    response_model=DeleteClusterResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses=error_responses(
        (UnauthorizedError, "Invalid or expired token"),
        (NotFoundError, "Cluster not found"),
        (ConflictError, "Cluster already deleting"),
    ),
)
async def delete_cluster(
    cluster_id: uuid.UUID,
    current_user: CurrentUser,
    cluster_service: ClusterServiceDep,
) -> DeleteClusterResponse:
    """Delete a KinD cluster (async via Celery)."""
    return await cluster_service.delete_cluster(current_user.id, cluster_id)
