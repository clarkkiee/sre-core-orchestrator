import enum
import uuid
from datetime import datetime

from sqlalchemy import ForeignKey, Enum, DateTime, Float, String, Index
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from app.models import Base


class MetricPhase(enum.StrEnum):
    BASELINE = "BASELINE"
    FAULT = "FAULT"
    RECOVERY = "RECOVERY"

class RawMetricSample(Base):
    __tablename__ = "raw_metric_samples"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    experiment_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("chaos_experiments.id", ondelete="CASCADE"),
        nullable=False
    )

    phase: Mapped[MetricPhase] = mapped_column(
        Enum(MetricPhase), nullable=False
    )

    metric_name: Mapped[str] = mapped_column(
        String(128), nullable=False
    )

    source: Mapped[str] = mapped_column(
        String(128), nullable=False
    )

    labels: Mapped[dict] = mapped_column(
        JSONB, nullable=False, default=dict
    )

    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

    value: Mapped[float] = mapped_column(
        Float, nullable=False
    )

    collected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    experiment = relationship("ChaosExperiment")

    __table_args__ = (
        Index(
            "ix_raw_metric_samples_experiment_phase_metric",
            "experiment_id", "phase", "metric_name",
        ),
        Index(
            "ix_raw_metric_samples_experiment_phase_metric_time",
            "experiment_id", "phase", "metric_name", "timestamp",
        )
    )
