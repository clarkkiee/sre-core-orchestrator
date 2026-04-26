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
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from app.models import Base


class ISOIndicator(enum.StrEnum):
    SYSTEM_AVAILABILITY = "SYSTEM_AVAILABILITY"  # RAv-1-G
    MEAN_DOWN_TIME = "MEAN_DOWN_TIME"             # RAv-2-G
    FAILURE_RATE = "FAILURE_RATE"                 # RMa-3-G
    MEAN_RECOVERY_TIME = "MEAN_RECOVERY_TIME"     # RRe-1-G


class MeasurementScope(enum.StrEnum):
    TARGET = "TARGET"
    PEER = "PEER"
    NAMESPACE_WIDE = "NAMESPACE_WIDE"


class EvaluationIndicator(Base):
    __tablename__ = "evaluation_indicators"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    evaluation_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("experiment_evaluations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    indicator: Mapped[ISOIndicator] = mapped_column(Enum(ISOIndicator), nullable=False)

    # Reuses the existing metricphase Postgres enum — create_type=False prevents recreation.
    # Nullable: MRT is computed across the fault→recovery boundary, not tied to a single phase.
    phase: Mapped[str | None] = mapped_column(
        Enum("BASELINE", "FAULT", "RECOVERY", name="metricphase", create_type=False),
        nullable=True,
    )

    scope: Mapped[MeasurementScope] = mapped_column(Enum(MeasurementScope), nullable=False)

    value: Mapped[float] = mapped_column(Float, nullable=False)
    sample_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    episode_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # String, not enum — formula versions accrete rapidly during research iterations.
    # e.g. "iso25023.rav1g.v1", "iso25023.rre1g.v1"
    formula_version: Mapped[str] = mapped_column(String(64), nullable=False)

    # Per-indicator supporting data for audit without schema churn.
    # e.g. {"down_episodes": [[t_start, t_end], ...], "step_seconds": 10}
    extra: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)

    computed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    evaluation = relationship("ExperimentEvaluation", back_populates="indicators")

    __table_args__ = (
        UniqueConstraint(
            "evaluation_id",
            "indicator",
            "phase",
            "scope",
            "formula_version",
            name="uq_evaluation_indicator_dimensions",
        ),
    )
