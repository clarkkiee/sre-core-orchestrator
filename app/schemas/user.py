import datetime

from pydantic import BaseModel, EmailStr


class UserResponse(BaseModel):
    id: str
    email: str
    is_admin: bool
    is_active: bool
    created_at: datetime.datetime
    updated_at: datetime.datetime


class UpdateUserRequest(BaseModel):
    email: EmailStr | None = None
    password: str | None = None
