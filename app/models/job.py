import enum
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import DateTime, Enum, ForeignKey, String, Text, func
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models import Base

if TYPE_CHECKING:
    from app.models.cluster import Cluster
    from app.models.deployment import Deployment
    from app.models.user import User


class JobType(enum.StrEnum):
    PROVISION_CLUSTER = "PROVISION_CLUSTER"
    RUN_PIPELINE = "RUN_PIPELINE"
    TEARDOWN_CLUSTER = "TEARDOWN_CLUSTER"
    RECONNECT_CLUSTER = "RECONNECT_CLUSTER"
    COLLECT_BASELINE_METRICS = "COLLECT_BASELINE_METRICS"
    COLLECT_CHAOS_METRICS = "COLLECT_CHAOS_METRICS"
    COLLECT_RECOVERY_METRICS = "COLLECT_RECOVERY_METRICS"
    CLEANUP_EXPIRED = "CLEANUP_EXPIRED"
    DEPLOY_APPLICATION = "DEPLOY_APPLICATION"


class JobStatus(enum.StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELED = "CANCELED"


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    cluster_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("clusters.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    deployment_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("deployments.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
        comment="Associated deployment, if this job is a deploy task",
    )

    job_type: Mapped[JobType] = mapped_column(
        Enum(JobType),
        nullable=False,
        index=True,
    )

    status: Mapped[JobStatus] = mapped_column(
        Enum(JobStatus),
        default=JobStatus.PENDING,
        nullable=False,
        index=True,
    )

    current_phase: Mapped[str | None] = mapped_column(
        String(100), nullable=True, comment="Current pipeline phase"
    )

    celery_task_id: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        unique=True,
        index=True,
        comment="The Celery task ID associated with this job",
    )

    result: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        nullable=True,
        comment="Job result data",
    )

    error_message: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )

    progress_percentage: Mapped[int | None] = mapped_column(
        nullable=True,
        comment="Job progress percentage",
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # Relationships
    tenant: Mapped["User"] = relationship("User", back_populates="jobs")
    cluster: Mapped["Cluster | None"] = relationship(
        "Cluster",
        back_populates="jobs",
    )
    deployment: Mapped["Deployment | None"] = relationship(
        "Deployment",
        back_populates="jobs",
    )

    @property
    def duration_seconds(self) -> float | None:
        if self.started_at:
            end_time = self.completed_at or datetime.now(UTC)
            return (end_time - self.started_at).total_seconds()
        return None

    def __repr__(self) -> str:
        return f"<Job(id={self.id}, type={self.job_type}, status={self.status})>"
