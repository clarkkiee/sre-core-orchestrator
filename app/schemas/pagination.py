from fastapi import Query
from typing import TypeVar
from dataclasses import dataclass
from fastapi_pagination import Params, Page
from fastapi_pagination.customization import CustomizedPage, UseFieldsAliases

T = TypeVar("T")

DefaultDataPage = CustomizedPage[
    Page[T],
    UseFieldsAliases(items="data")
]

@dataclass(frozen=True)
class Pagination:
    limit: int
    offset: int
    

class PaginationParams(Params):
    size: int = Query(default=20, ge=1, le=100, description="items per page")
    
    def to_pagination(self) -> Pagination:
        return Pagination(
            limit=self.size,
            offset=self.size * (self.page - 1)
        )
