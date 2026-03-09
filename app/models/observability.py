import enum
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    Boolean,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models import Base

if TYPE_CHECKING:
    from app.models.cluster import Cluster
    from app.models.deployment import Deployment
    from app.models.user import User


class CollectionPhase(enum.StrEnum):
    BASELINE = "BASELINE"
    CHAOS = "CHAOS"
    RECOVERY = "RECOVERY"


class SessionStatus(enum.StrEnum):
    PENDING = "PENDING"
    COLLECTING = "COLLECTING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class ObservabilitySession(Base):
    __tablename__ = "observability_sessions"

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

    deployment_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("deployments.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    phase: Mapped[CollectionPhase] = mapped_column(
        Enum(CollectionPhase),
        nullable=False,
    )

    status: Mapped[SessionStatus] = mapped_column(
        Enum(SessionStatus),
        nullable=False,
        default=SessionStatus.PENDING,
        index=True,
    )

    status_message: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )

    target_namespace: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        default="default",
    )

    collection_duration_seconds: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=300,
    )

    collection_interval_seconds: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=15,
    )

    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        onupdate=func.now(),
        server_default=func.now(),
    )

    # Relationships
    tenant: Mapped["User"] = relationship("User")
    cluster: Mapped["Cluster"] = relationship("Cluster")
    deployment: Mapped["Deployment | None"] = relationship("Deployment")
    snapshots: Mapped[list["MetricSnapshot"]] = relationship(
        "MetricSnapshot",
        back_populates="session",
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        return (
            f"<ObservabilitySession("
            f"id={self.id}, phase={self.phase}, "
            f"status={self.status})>"
        )


class MetricSnapshot(Base):
    __tablename__ = "metric_snapshots"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )

    session_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("observability_sessions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    sli_name: Mapped[str] = mapped_column(String(50), nullable=False)

    sub_characteristics: Mapped[str] = mapped_column(String(50), nullable=False)

    display_name: Mapped[str] = mapped_column(String(200), nullable=False)

    promql_query: Mapped[str] = mapped_column(Text, nullable=False)

    value: Mapped[float | None] = mapped_column(Float, nullable=False)

    unit: Mapped[str] = mapped_column(String(50), nullable=False)

    slo_target: Mapped[float] = mapped_column(Float, nullable=False)

    slo_operator: Mapped[str] = mapped_column(String(10), nullable=False)

    slo_met: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    collected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

    raw_response: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    # Relationship
    session: Mapped["ObservabilitySession"] = relationship(
        "ObservabilitySession", back_populates="snapshots"
    )

    def __repr__(self) -> str:
        return (
            f"<MetricSnapshot(id={self.id}, sli={self.sli_name}, value={self.value})>"
        )


class ReliabilityReport(Base):
    __tablename__ = "reliability_reports"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
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

    deployment_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("deployments.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    baseline_session_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("observability_sessions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    chaos_session_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("observability_sessions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    recovery_session_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("observability_sessions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    overall_score: Mapped[float] = mapped_column(Float, nullable=False)

    availability_score: Mapped[float] = mapped_column(Float, nullable=False)

    faultlessness_score: Mapped[float] = mapped_column(Float, nullable=False)

    fault_tolerance_score: Mapped[float] = mapped_column(Float, nullable=False)

    recoverability_score: Mapped[float] = mapped_column(Float, nullable=False)

    details: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationships
    tenant: Mapped["User"] = relationship("User")
    cluster: Mapped["Cluster"] = relationship("Cluster")
    deployment: Mapped["Deployment | None"] = relationship("Deployment")
    baseline_session: Mapped["ObservabilitySession"] = relationship(
        "ObservabilitySession", foreign_keys=[baseline_session_id]
    )
    chaos_session: Mapped["ObservabilitySession"] = relationship(
        "ObservabilitySession", foreign_keys=[chaos_session_id]
    )
    recovery_session: Mapped["ObservabilitySession"] = relationship(
        "ObservabilitySession", foreign_keys=[recovery_session_id]
    )

    def __repr__(self) -> str:
        return f"<ReliabilityReport(id={self.id}, overall_score={self.overall_score})>"
