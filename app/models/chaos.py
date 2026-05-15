"""Chaos Experiment Model"""

import enum
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from app.models import Base


class ExperimentType(enum.StrEnum):
    POD_DELETE = "POD_DELETE"
    POD_CPU_HOG = "POD_CPU_HOG"
    POD_MEMORY_HOG = "POD_MEMORY_HOG"
    POD_NETWORK_LATENCY = "POD_NETWORK_LATENCY"
    POD_NETWORK_LOSS = "POD_NETWORK_LOSS"


class ChaosExperimentStatus(enum.StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    STOPPED = "STOPPED"


class ChaosExperiment(Base):
    __tablename__ = "chaos_experiments"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id"), nullable=False
    )

    cluster_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("clusters.id"), nullable=False
    )

    deployment_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("deployments.id"), nullable=False
    )

    experiment_type: Mapped[ExperimentType] = mapped_column(
        Enum(ExperimentType), nullable=False
    )

    target_namespace: Mapped[str] = mapped_column(String(253), nullable=False)

    target_label: Mapped[str] = mapped_column(String(253), nullable=False)

    status: Mapped[ChaosExperimentStatus] = mapped_column(
        Enum(ChaosExperimentStatus),
        nullable=False,
        default=ChaosExperimentStatus.PENDING,
    )

    status_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    duration_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=60)

    chaos_engine_name: Mapped[str | None] = mapped_column(
        String(253),
        nullable=True,
    )

    # Unix timestamp written by the evaluation pipeline immediately after
    # poll_experiment_result returns, while the chaos-exporter gauge is still
    # pointing at this engine.  Stored here so resolve_fault_window never needs
    # to query VM post-hoc against a gauge that may already be overwritten.
    chaos_injected_time: Mapped[float | None] = mapped_column(Float, nullable=True)

    baseline_start: Mapped[float | None] = mapped_column(Float, nullable=True)
    baseline_end: Mapped[float | None] = mapped_column(Float, nullable=True)
    recovery_start: Mapped[float | None] = mapped_column(Float, nullable=True)
    recovery_end: Mapped[float | None] = mapped_column(Float, nullable=True)

    campaign_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("chaos_campaigns.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )

    configuration: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    baseline_metrics: Mapped[dict[str, Any] | None] = mapped_column(
        JSON, nullable=True
    )

    result: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    tenant = relationship("User", back_populates="chaos_experiments")
    cluster = relationship("Cluster", back_populates="chaos_experiments")
    deployment = relationship("Deployment", back_populates="chaos_experiments")
    campaign = relationship("ChaosCampaign", back_populates="experiments")
    evaluation = relationship(
        "ExperimentEvaluation",
        back_populates="experiment",
        uselist=False,
        cascade="all, delete-orphan",
    )
