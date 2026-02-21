from app.exceptions.errors import Conflict
from app.models.user import User
from app.repositories.user import UserRepository
from app.schemas.user import UpdateUserRequest, UserResponse
from app.utils.security import hash_password


class UserService:
    def __init__(self, user_repository: UserRepository):
        self.user_repository = user_repository

    async def update_profile(self, current_user: User, payload: UpdateUserRequest) -> UserResponse:
        fields: dict[str, object] = {}

        if payload.email is not None and payload.email != current_user.email:
            existing = await self.user_repository.get_by_email(payload.email)
            if existing:
                raise Conflict("Email already in use")
            fields["email"] = payload.email

        if payload.password is not None:
            fields["hashed_password"] = hash_password(payload.password)

        if fields:
            current_user = await self.user_repository.update(current_user, **fields)

        return UserResponse(
            id=str(current_user.id),
            email=current_user.email,
            is_admin=current_user.is_admin,
            is_active=current_user.is_active,
            created_at=current_user.created_at,
            updated_at=current_user.updated_at,
        )
