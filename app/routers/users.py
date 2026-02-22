from fastapi import APIRouter

from app.dependencies import CurrentUser, UserServiceDep
from app.exceptions.errors import ConflictError, UnauthorizedError, error_responses
from app.schemas.user import UpdateUserRequest, UserResponse

router = APIRouter(prefix="/users", tags=["users"])


@router.patch(
    "/me",
    response_model=UserResponse,
    responses=error_responses(
        (UnauthorizedError, "Invalid or expired token"),
        (ConflictError, "Email already in use"),
    ),
)
async def update_profile(
    payload: UpdateUserRequest,
    current_user: CurrentUser,
    user_service: UserServiceDep,
) -> UserResponse:
    """Update the currently authenticated user's profile."""
    return await user_service.update_profile(current_user, payload)
