from datetime import UTC, datetime, timedelta
from typing import Any

import jwt as pyjwt

from app.utils.config import settings


def create_access_token(subject: str, is_admin: bool) -> str:
    now = datetime.now(UTC)
    expire = now + timedelta(minutes=settings.JWT_ACCESS_TOKEN_EXPIRES_MINUTES)
    payload = {
        "iat": now,
        "exp": expire,
        "sub": subject,
        "is_admin": is_admin,
    }
    encoded: str = pyjwt.encode(
        payload, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM
    )
    return encoded


def decode_token(token: str) -> dict[str, Any]:
    result: dict[str, Any] = pyjwt.decode(
        token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM]
    )
    return result
