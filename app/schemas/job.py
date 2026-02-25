"""Pydantic schemas for job endpoints."""

import datetime
from typing import Any

from pydantic import BaseModel


class JobResponse(BaseModel):
    id: str
    cluster_id: str
    job_type: str
    status: str
    current_phase: str | None
    progress_percentage: int | None
    error_message: str | None
    result: dict[str, Any] | None
    created_at: datetime.datetime
    started_at: datetime.datetime | None
    completed_at: datetime.datetime | None
