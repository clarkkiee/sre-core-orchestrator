from app.exceptions.errors import Forbidden, Unauthorized, Conflict
from app.repositories.user import UserRepository
from app.schemas.auth import LoginResponse, RegisterRequest, RegisterResponse
from app.models.user import User
from app.utils.jwt import create_access_token
from app.utils.security import hash_password, verify_password


class AuthService:
    def __init__(self, user_repository: UserRepository):
        self.user_repository = user_repository

    async def login(self, email: str, password: str) -> LoginResponse:
        user = await self.user_repository.get_by_email(email)

        if not user or not verify_password(password, user.hashed_password):
            raise Unauthorized("Invalid credentials")

        if not user.is_active:
            raise Forbidden("User account is disabled")

        token = create_access_token(subject=str(user.id))
        return LoginResponse(access_token=token)

    async def register(self, payload: RegisterRequest) -> RegisterResponse:
        existing_user = await self.user_repository.get_by_email(payload.email)

        if existing_user:
            raise Conflict("User already registered")

        new_user = User(
            email=payload.email,
            hashed_password=hash_password(payload.password),
        )

        created_user = await self.user_repository.create(new_user)

        return RegisterResponse(id=str(created_user.id), email=created_user.email)
