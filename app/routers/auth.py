from fastapi import APIRouter, status

from app.dependencies import AuthServiceDep, CurrentUser
from app.exceptions.errors import (
    ConflictError,
    ForbiddenError,
    UnauthorizedError,
    error_responses,
)
from app.schemas.auth import (
    LoginRequest,
    LoginResponse,
    RegisterRequest,
    RegisterResponse,
)
from app.schemas.user import UserResponse

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post(
    "/login",
    response_model=LoginResponse,
    status_code=status.HTTP_200_OK,
    responses=error_responses(
        (UnauthorizedError, "Invalid credentials"),
        (ForbiddenError, "User account is disabled"),
    ),
)
async def login(
    payload: LoginRequest,
    auth_service: AuthServiceDep,
) -> LoginResponse:
    """Authenticate a user and return a JWT access token."""
    return await auth_service.login(payload.email, payload.password)


@router.post(
    "/register",
    response_model=RegisterResponse,
    status_code=status.HTTP_201_CREATED,
    responses=error_responses(
        (ConflictError, "User already registered"),
    ),
)
async def register(
    payload: RegisterRequest,
    auth_service: AuthServiceDep,
) -> RegisterResponse:
    """Register a new user."""
    return await auth_service.register(payload)


@router.get(
    "/me",
    response_model=UserResponse,
    responses=error_responses(
        (UnauthorizedError, "Invalid or expired token"),
    ),
)
async def me(current_user: CurrentUser) -> UserResponse:
    """Get the currently authenticated user's profile."""
    return UserResponse(
        id=str(current_user.id),
        email=current_user.email,
        is_admin=current_user.is_admin,
        is_active=current_user.is_active,
        created_at=current_user.created_at,
        updated_at=current_user.updated_at,
    )
