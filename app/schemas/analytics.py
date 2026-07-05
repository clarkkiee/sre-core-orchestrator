from pydantic import BaseModel

class StatusCount(BaseModel):
    status: str
    count: int

class EntityStats(BaseModel):
    total: int
    by_status: list[StatusCount]

class AnalyticResponse(BaseModel):
    clusters: EntityStats
    deployments: EntityStats
    chaos_experiments: EntityStats
    chaos_campaigns: EntityStats