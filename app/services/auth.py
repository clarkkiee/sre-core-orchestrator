from app.exceptions.errors import ConflictError, ForbiddenError, UnauthorizedError
from app.models.user import User
from app.repositories.user import UserRepository
from app.schemas.auth import LoginResponse, RegisterRequest, RegisterResponse
from app.utils.jwt import create_access_token
from app.utils.security import hash_password, verify_password


class AuthService:
    def __init__(self, user_repository: UserRepository) -> None:
        self.user_repository = user_repository

    async def login(self, email: str, password: str) -> LoginResponse:
        user = await self.user_repository.get_by_email(email)

        if not user or not verify_password(password, user.hashed_password):
            msg = "Invalid credentials"
            raise UnauthorizedError(msg)

        if not user.is_active:
            msg = "User account is disabled"
            raise ForbiddenError(msg)

        token = create_access_token(subject=str(user.id))
        return LoginResponse(access_token=token)

    async def register(self, payload: RegisterRequest) -> RegisterResponse:
        existing_user = await self.user_repository.get_by_email(payload.email)

        if existing_user:
            msg = "User already registered"
            raise ConflictError(msg)

        new_user = User(
            email=payload.email,
            hashed_password=hash_password(payload.password),
        )

        created_user = await self.user_repository.create(new_user)

        return RegisterResponse(id=str(created_user.id), email=created_user.email)
