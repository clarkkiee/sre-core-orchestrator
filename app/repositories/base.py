from typing import Any, Generic, TypeVar

from sqlalchemy import Select, select, func
from sqlalchemy.ext.asyncio import AsyncSession
from app.schemas.pagination import Pagination

ModelT = TypeVar("ModelT")

class BaseRepository(Generic[ModelT]):
    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    async def create(self, obj: ModelT) -> ModelT:
        self.db.add(obj)
        await self.db.flush()
        await self.db.refresh(obj)
        return obj

    async def update(self, obj: ModelT, **fields: Any) -> ModelT:
        for key, value in fields.items():
            setattr(obj, key, value)
        await self.db.flush()
        await self.db.refresh(obj)
        return obj

    async def paginate(
        self,
        stmt: Select[tuple[ModelT]],
        pagination: Pagination,
    ) -> tuple[list[ModelT], int]:
        total = await self.db.scalar(
            select(func.count()).select_from(stmt.subquery())
        ) or 0
        
        result = await self.db.execute(
            stmt.limit(pagination.limit).offset(pagination.offset)
        )
        
        return list(result.scalars().all()), total