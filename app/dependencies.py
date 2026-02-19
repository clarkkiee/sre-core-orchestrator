"""Shared FastAPI dependencies."""
import uuid
from typing import Annotated

from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt.exceptions import InvalidTokenError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.exceptions.errors import Unauthorized
from app.models.user import User
from app.repositories.user import UserRepository
from app.utils.jwt import decode_token

bearer_scheme = HTTPBearer()


async def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(bearer_scheme)],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> User:
    """Decode the JWT and return the authenticated User.

    Use as a dependency on protected endpoints:
        current_user: Annotated[User, Depends(get_current_user)]
    """
    try:
        payload = decode_token(credentials.credentials)
        user_id_str: str | None = payload.get("sub")
        if user_id_str is None:
            raise Unauthorized("Token missing subject claim")
        user_id = uuid.UUID(user_id_str)
    except (InvalidTokenError, ValueError):
        raise Unauthorized("Invalid or expired token")

    repo = UserRepository()
    user = await repo.get_by_id(db, user_id)
    if user is None or not user.is_active:
        raise Unauthorized("User not found or inactive")
    return user
