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
from app.repositories.user import UserRepository
from app.services.auth import AuthService
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
