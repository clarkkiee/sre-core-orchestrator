"""Database models package."""

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Base class for all database models."""


# Import all models here for Alembic to detect them
from app.models.cluster import Cluster  # noqa: E402
from app.models.deployment import (  # noqa: E402
    Deployment,
    DeploymentStatus,
    DeployStrategy,
)
from app.models.job import Job, JobStatus, JobType  # noqa: E402
from app.models.observability import (  # noqa: E402
    MetricSnapshot,
    ObservabilitySession,
    ReliabilityReport,
)
from app.models.user import User  # noqa: E402

__all__ = [
    "Base",
    "Cluster",
    "DeployStrategy",
    "Deployment",
    "DeploymentStatus",
    "Job",
    "JobStatus",
    "JobType",
    "MetricSnapshot",
    "ObservabilitySession",
    "ReliabilityReport",
    "User",
]
