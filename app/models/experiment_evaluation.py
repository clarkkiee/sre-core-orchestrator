import enum
import uuid
from datetime import datetime

from sqlalchemy import DateTime, Enum, Float, ForeignKey, String, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from app.models import Base


class EvaluationStatus(enum.StrEnum):
    SUCCESS = "SUCCESS"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"


class ExperimentEvaluation(Base):
    __tablename__ = "experiment_evaluations"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    experiment_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("chaos_experiments.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )

    baseline_start: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    baseline_end: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    fault_start: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    fault_end: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    recovery_start: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    recovery_end: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

    litmus_probe_percentage: Mapped[float | None] = mapped_column(
        Float, nullable=True
    )

    status: Mapped[EvaluationStatus] = mapped_column(
        Enum(EvaluationStatus), nullable=False, default=EvaluationStatus.SUCCESS
    )
    status_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    evaluator_version: Mapped[str] = mapped_column(
        String(32), nullable=False, default="v1"
    )

    evaluated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    experiment = relationship("ChaosExperiment", back_populates="evaluation")
    indicators = relationship(
        "EvaluationIndicator",
        back_populates="evaluation",
        cascade="all, delete-orphan",
    )
