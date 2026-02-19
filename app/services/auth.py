from sqlalchemy.ext.asyncio import AsyncSession

from app.exceptions.errors import Forbidden, Unauthorized
from app.repositories.user import UserRepository
from app.schemas.auth import LoginResponse
from app.utils.jwt import create_access_token
from app.utils.security import verify_password


class AuthService:
    def __init__(self, user_repository: UserRepository):
        self.user_repository = user_repository

    async def login(self, db: AsyncSession, email: str, password: str) -> LoginResponse:
        user = await self.user_repository.get_by_email(db, email)

        if not user or not verify_password(password, user.hashed_password):
            raise Unauthorized("Invalid credentials")

        if not user.is_active:
            raise Forbidden("User account is disabled")

        token = create_access_token(subject=str(user.id))
        return LoginResponse(access_token=token)
