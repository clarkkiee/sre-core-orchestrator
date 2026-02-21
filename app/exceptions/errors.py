from fastapi import HTTPException, status
from pydantic import BaseModel


class ErrorResponse(BaseModel):
    detail: str


class APIError(HTTPException):
    status_code: int

    def __init__(self, detail: str):
        super().__init__(
            status_code=self.status_code,
            detail=detail
        )

class NotFound(APIError):
    status_code = status.HTTP_404_NOT_FOUND

class Forbidden(APIError):
    status_code = status.HTTP_403_FORBIDDEN

class Unauthorized(APIError):
    status_code = status.HTTP_401_UNAUTHORIZED

class Conflict(APIError):
    status_code = status.HTTP_409_CONFLICT

class InternalServerError(APIError):
    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR


def error_responses(
    *errors: tuple[type[APIError], str],
) -> dict[int, dict]:
    """Build an OpenAPI responses dict from APIError subclasses.

    Usage:
        responses=error_responses(
            (Unauthorized, "Invalid credentials"),
            (Forbidden, "User account is disabled"),
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