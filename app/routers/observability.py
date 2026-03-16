"""Observability API endpoints."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Query, status

from app.dependencies import CurrentUser, ObservabilityServiceDep
from app.exceptions.errors import (
    ConflictError,
    NotFoundError,
    UnauthorizedError,
    error_responses,
)
from app.infrastructure.metrics.sli_registry import SLI_REGISTRY
from app.schemas.observability import (
    GenerateReportRequest,
    ReliabilityReportResponse,
    SessionListResponse,
    SessionMetricsResponse,
    SessionResponse,
    SessionWithJobResponse,
    SLIDefinitionResponse,
    StartCollectionRequest,
)

router = APIRouter(prefix="/observability", tags=["observability"])


@router.post(
    "/sessions",
    response_model=SessionWithJobResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses=error_responses(
        (UnauthorizedError, "Invalid or expired token"),
        (NotFoundError, "Cluster or deployment not found"),
        (ConflictError, "Cluster is not ready or misconfigured"),
    ),
)
async def start_collection(
    payload: StartCollectionRequest,
    current_user: CurrentUser,
    observability_service: ObservabilityServiceDep,
) -> SessionWithJobResponse:
    """Start metric collection for a deployment (async via Celery)."""
    return await observability_service.start_collection(current_user.id, payload)


@router.get(
    "/sessions",
    response_model=SessionListResponse,
    responses=error_responses(
        (UnauthorizedError, "Invalid or expired token"),
    ),
)
async def list_sessions(
    current_user: CurrentUser,
    observability_service: ObservabilityServiceDep,
    cluster_id: Annotated[uuid.UUID, Query(description="Filter by cluster")],
) -> SessionListResponse:
    """List observability sessions for a cluster."""
    return await observability_service.list_sessions(current_user.id, cluster_id)


@router.get(
    "/sessions/{session_id}",
    response_model=SessionResponse,
    responses=error_responses(
        (UnauthorizedError, "Invalid or expired token"),
        (NotFoundError, "Session not found"),
    ),
)
async def get_session(
    session_id: uuid.UUID,
    current_user: CurrentUser,
    observability_service: ObservabilityServiceDep,
) -> SessionResponse:
    """Get a specific observability session."""
    return await observability_service.get_session(current_user.id, session_id)


@router.get(
    "/sessions/{session_id}/metrics",
    response_model=SessionMetricsResponse,
    responses=error_responses(
        (UnauthorizedError, "Invalid or expired token"),
        (NotFoundError, "Session not found"),
    ),
)
async def get_session_metrics(
    session_id: uuid.UUID,
    current_user: CurrentUser,
    observability_service: ObservabilityServiceDep,
) -> SessionMetricsResponse:
    """Get metric snapshots for a session."""
    return await observability_service.get_session_metrics(current_user.id, session_id)


@router.post(
    "/reports",
    response_model=ReliabilityReportResponse,
    status_code=status.HTTP_201_CREATED,
    responses=error_responses(
        (UnauthorizedError, "Invalid or expired token"),
        (NotFoundError, "Session not found"),
        (ConflictError, "Session not completed"),
    ),
)
async def generate_report(
    payload: GenerateReportRequest,
    current_user: CurrentUser,
    observability_service: ObservabilityServiceDep,
) -> ReliabilityReportResponse:
    """Generate a reliability report comparing sessions."""
    return await observability_service.generate_report(current_user.id, payload)


@router.get(
    "/reports/{report_id}",
    response_model=ReliabilityReportResponse,
    responses=error_responses(
        (UnauthorizedError, "Invalid or expired token"),
        (NotFoundError, "Report not found"),
    ),
)
async def get_report(
    report_id: uuid.UUID,
    current_user: CurrentUser,
    observability_service: ObservabilityServiceDep,
) -> ReliabilityReportResponse:
    """Get a reliability report."""
    return await observability_service.get_report(current_user.id, report_id)


@router.get(
    "/sli-definitions",
    response_model=list[SLIDefinitionResponse],
)
async def list_sli_definitions() -> list[SLIDefinitionResponse]:
    """List all SLI definitions (static registry, no auth)."""
    return [
        SLIDefinitionResponse(
            id=sli.id,
            name=sli.name,
            sub_characteristics=sli.sub_characteristics,
            promql_template=sli.promql_template,
            unit=sli.unit,
            slo_target=sli.slo_target,
            slo_operator=sli.slo_operator,
            description=sli.description,
        )
        for sli in SLI_REGISTRY.values()
    ]
