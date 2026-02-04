"""Database models package."""

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Base class for all database models."""

    pass


# Import all models here for Alembic to detect them
from app.models.user import User
from app.models.cluster import Cluster
from app.models.job import Job, JobType, JobStatus

__all__ = [
    "Base",
    "User",
    "Cluster",
    "Job",
    "JobType",
    "JobStatus",
]
