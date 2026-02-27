"""Centralized FastAPI dependency injection wiring."""

import uuid
from typing import Annotated

from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt.exceptions import InvalidTokenError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.exceptions.errors import UnauthorizedError
from app.models.user import User
from app.repositories.cluster import ClusterRepository
from app.repositories.deployment import DeploymentRepository
from app.repositories.job import JobRepository
from app.repositories.user import UserRepository
from app.services.auth import AuthService
from app.services.cluster import ClusterService
from app.services.deployment import DeploymentService
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
