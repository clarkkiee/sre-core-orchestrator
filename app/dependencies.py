"""Centralized FastAPI dependency injection wiring."""

import uuid
from functools import lru_cache
from typing import Annotated

from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt.exceptions import InvalidTokenError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.exceptions.errors import ForbiddenError, UnauthorizedError
from app.infrastructure.metrics.catalog import MetricCatalog
from app.infrastructure.metrics.client_factory import VictoriaMetricsClientFactory
from app.infrastructure.metrics.query_engine import MetricsQueryEngine
from app.models.user import User
from app.repositories.campaign import CampaignRepository
from app.repositories.chaos import ChaosRepository
from app.repositories.cluster import ClusterRepository
from app.repositories.deployment import DeploymentRepository
from app.repositories.job import JobRepository
from app.repositories.observability import ObservabilityRepository
from app.repositories.raw_metric_sample import RawMetricSampleRepository
from app.repositories.user import UserRepository
from app.services.auth import AuthService
from app.services.campaign import CampaignService
from app.services.chaos import ChaosService
from app.services.cluster import ClusterService
from app.services.deployment import DeploymentService
from app.services.observability import ObservabilityService
from app.services.user import UserService
from app.utils.jwt import decode_token

bearer_scheme = HTTPBearer()

# Database session
DbSession = Annotated[AsyncSession, Depends(get_db)]


# Repositories
def get_user_repository(db: DbSession) -> UserRepository:
    return UserRepository(db)


UserRepositoryDep = Annotated[UserRepository, Depends(get_user_repository)]


# Services
def get_auth_service(user_repository: UserRepositoryDep) -> AuthService:
    return AuthService(user_repository=user_repository)


AuthServiceDep = Annotated[AuthService, Depends(get_auth_service)]


def get_user_service(user_repository: UserRepositoryDep) -> UserService:
    return UserService(user_repository=user_repository)


UserServiceDep = Annotated[UserService, Depends(get_user_service)]


# Cluster / Job repositories
def get_cluster_repository(db: DbSession) -> ClusterRepository:
    return ClusterRepository(db)


def get_job_repository(db: DbSession) -> JobRepository:
    return JobRepository(db)


ClusterRepositoryDep = Annotated[ClusterRepository, Depends(get_cluster_repository)]
JobRepositoryDep = Annotated[JobRepository, Depends(get_job_repository)]


# Cluster service
def get_cluster_service(
    cluster_repository: ClusterRepositoryDep,
    job_repository: JobRepositoryDep,
) -> ClusterService:
    return ClusterService(
        cluster_repository=cluster_repository,
        job_repository=job_repository,
    )


ClusterServiceDep = Annotated[ClusterService, Depends(get_cluster_service)]


# Deployment repository
def get_deployment_repository(db: DbSession) -> DeploymentRepository:
    return DeploymentRepository(db)


DeploymentRepositoryDep = Annotated[
    DeploymentRepository, Depends(get_deployment_repository)
]


# Deployment service
def get_deployment_service(
    deployment_repository: DeploymentRepositoryDep,
    cluster_repository: ClusterRepositoryDep,
    job_repository: JobRepositoryDep,
) -> DeploymentService:
    return DeploymentService(
        deployment_repository=deployment_repository,
        cluster_repository=cluster_repository,
        job_repository=job_repository,
    )


DeploymentServiceDep = Annotated[DeploymentService, Depends(get_deployment_service)]


# Auth dependencies
async def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(bearer_scheme)],
    user_repository: UserRepositoryDep,
) -> User:
    """Decode the JWT and return the authenticated User."""
    try:
        payload = decode_token(credentials.credentials)
        user_id_str: str | None = payload.get("sub")
        if user_id_str is None:
            msg = "Token missing subject claim"
            raise UnauthorizedError(msg)
        user_id = uuid.UUID(user_id_str)
    except (InvalidTokenError, ValueError) as err:
        msg = "Invalid or expired token"
        raise UnauthorizedError(msg) from err

    user = await user_repository.get_by_id(user_id)
    if user is None or not user.is_active:
        msg = "User not found or inactive"
        raise UnauthorizedError(msg)
    return user


CurrentUser = Annotated[User, Depends(get_current_user)]


async def get_admin_user(current_user: CurrentUser) -> User:
    """Require the authenticated user to be an admin."""
    if not current_user.is_admin:
        msg = "Admin access required"
        raise ForbiddenError(msg)
    return current_user


AdminUser = Annotated[User, Depends(get_admin_user)]


# Observability repository
def get_observability_repository(db: DbSession) -> ObservabilityRepository:
    return ObservabilityRepository(db)


ObservabilityRepositoryDep = Annotated[
    ObservabilityRepository, Depends(get_observability_repository)
]


# Observability service
def get_observability_service(
    observability_repository: ObservabilityRepositoryDep,
    cluster_repository: ClusterRepositoryDep,
    deployment_repository: DeploymentRepositoryDep,
    job_repository: JobRepositoryDep,
) -> ObservabilityService:
    return ObservabilityService(
        observability_repository=observability_repository,
        cluster_repository=cluster_repository,
        deployment_repository=deployment_repository,
        job_repository=job_repository,
    )


ObservabilityServiceDep = Annotated[
    ObservabilityService, Depends(get_observability_service)
]


# Chaos Repository
def get_chaos_repository(db: DbSession) -> ChaosRepository:
    return ChaosRepository(db)


ChaosRepositoryDep = Annotated[ChaosRepository, Depends(get_chaos_repository)]


# Chaos Service
def get_chaos_service(
    chaos_repository: ChaosRepositoryDep,
    cluster_repository: ClusterRepositoryDep,
    deployment_repository: DeploymentRepositoryDep,
    job_repository: JobRepositoryDep,
) -> ChaosService:
    return ChaosService(
        chaos_repository=chaos_repository,
        cluster_repository=cluster_repository,
        deployment_repository=deployment_repository,
        job_repository=job_repository,
    )


ChaosServiceDep = Annotated[ChaosService, Depends(get_chaos_service)]


# Campaign Repository
def get_campaign_repository(db: DbSession) -> CampaignRepository:
    return CampaignRepository(db)


CampaignRepositoryDep = Annotated[
    CampaignRepository, Depends(get_campaign_repository)
]


# Campaign Service
def get_campaign_service(
    campaign_repository: CampaignRepositoryDep,
    cluster_repository: ClusterRepositoryDep,
    deployment_repository: DeploymentRepositoryDep,
    job_repository: JobRepositoryDep,
) -> CampaignService:
    return CampaignService(
        campaign_repository=campaign_repository,
        cluster_repository=cluster_repository,
        deployment_repository=deployment_repository,
        job_repository=job_repository,
    )


CampaignServiceDep = Annotated[CampaignService, Depends(get_campaign_service)]

@lru_cache(maxsize=1)
def get_metric_catalog() -> MetricCatalog:
    return MetricCatalog.load_from_dir()

def get_raw_metric_sample_repository(db: DbSession) -> RawMetricSampleRepository:
    return RawMetricSampleRepository(db)

def get_victoriametrics_factory(
    cluster_repo: Annotated[ClusterRepository, Depends(get_cluster_repository)],
    experiment_repo: Annotated[ChaosRepository, Depends(get_chaos_repository)]
) -> VictoriaMetricsClientFactory:
    return VictoriaMetricsClientFactory(
        cluster_repo=cluster_repo,
        experiment_repo=experiment_repo
    )

def get_metrics_query_engine(
    factory: Annotated[
        VictoriaMetricsClientFactory, Depends(get_victoriametrics_factory)
    ],
    catalog: Annotated[MetricCatalog, Depends(get_metric_catalog)],
    repo: Annotated[
        RawMetricSampleRepository, Depends(get_raw_metric_sample_repository)
    ]
) -> MetricsQueryEngine:
    return MetricsQueryEngine(
        client_factory=factory,
        catalog=catalog,
        repo=repo
    )

MetricsQueryEngineDep = Annotated[
    MetricsQueryEngine, Depends(get_metrics_query_engine)
]
