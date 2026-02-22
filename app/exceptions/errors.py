from typing import Any

from fastapi import HTTPException, status
from pydantic import BaseModel


class ErrorResponse(BaseModel):
    detail: str


class APIError(HTTPException):
    status_code: int

    def __init__(self, detail: str) -> None:
        super().__init__(
            status_code=self.status_code,
            detail=detail,
        )


class NotFoundError(APIError):
    status_code = status.HTTP_404_NOT_FOUND


class ForbiddenError(APIError):
    status_code = status.HTTP_403_FORBIDDEN


class UnauthorizedError(APIError):
    status_code = status.HTTP_401_UNAUTHORIZED


class ConflictError(APIError):
    status_code = status.HTTP_409_CONFLICT


class InternalServerError(APIError):
    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR


def error_responses(
    *errors: tuple[type[APIError], str],
) -> dict[int | str, dict[str, Any]]:
    """Build an OpenAPI responses dict from APIError subclasses.

    Usage:
        responses=error_responses(
            (UnauthorizedError, "Invalid credentials"),
            (ForbiddenError, "User account is disabled"),
        )
    """
    return {
        error_cls.status_code: {
            "description": message,
            "model": ErrorResponse,
            "content": {
                "application/json": {
                    "example": {"detail": message},
                },
            },
        }
        for error_cls, message in errors
    }
