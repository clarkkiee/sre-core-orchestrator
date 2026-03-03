"""Admin API endpoints for super-admin cluster management."""

import uuid

from fastapi import APIRouter, status

from app.dependencies import ClusterServiceDep
from app.exceptions.errors import (
    ConflictError,
    ForbiddenError,
    NotFoundError,
    UnauthorizedError,
    error_responses,
)
from app.schemas.cluster import (
    AdminClusterListResponse,
    ReconnectClusterResponse,
)

router = APIRouter(prefix="/admin", tags=["admin"])


@router.get(
    "/clusters",
    response_model=AdminClusterListResponse,
    responses=error_responses(
        (UnauthorizedError, "Invalid or expired token"),
        (ForbiddenError, "Admin access required"),
    ),
)
async def admin_list_clusters(
    cluster_service: ClusterServiceDep,
) -> AdminClusterListResponse:
    """List ALL clusters across all tenants (admin only)."""
    return await cluster_service.admin_list_clusters()


@router.post(
    "/clusters/{cluster_id}/reconnect",
    response_model=ReconnectClusterResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses=error_responses(
        (UnauthorizedError, "Invalid or expired token"),
        (ForbiddenError, "Admin access required"),
        (NotFoundError, "Cluster not found"),
        (ConflictError, "Cluster cannot be reconnected in current state"),
    ),
)
async def admin_reconnect_cluster(
    cluster_id: uuid.UUID,
    cluster_service: ClusterServiceDep,
) -> ReconnectClusterResponse:
    """Reconnect any cluster regardless of tenant (admin only)."""
    return await cluster_service.admin_reconnect_cluster(cluster_id)
