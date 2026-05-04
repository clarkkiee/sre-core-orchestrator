import enum
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from app.models import Base
from app.models.evaluation_indicator import ISOIndicator


class ProbeType(enum.StrEnum):
    HTTP = "HTTP"
    CMD = "CMD"
    PROM = "PROM"
    K8S = "K8S"

class ProbeMode(enum.StrEnum):
    SOT = "SOT"
    EOT = "EOT"
    EDGE = "EDGE"
    CONTINUOUS = "CONTINUOUS"
    ON_CHAOS = "ON_CHAOS"

class ProbeVerdict(enum.StrEnum):
    PASSED = "PASSED"
    FAILED = "FAILED"
    NA = "NA"

class ProbeResult(Base):
    __tablename__ = "probe_results"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True,
        default=uuid.uuid4
    )

    evaluation_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("experiment_evaluations.id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )

    probe_name: Mapped[str] = mapped_column(
        String(120),
        nullable=False
    )

    probe_type: Mapped[ProbeType] = mapped_column(
        Enum(ProbeType),
        nullable=False
    )

    probe_mode: Mapped[ProbeMode] = mapped_column(
        Enum(ProbeMode),
        nullable=False
    )

    verdict: Mapped[ProbeVerdict] = mapped_column(
        Enum(ProbeVerdict),
        nullable=False
    )

    success_percentage: Mapped[float | None] = mapped_column(
        Float, nullable=True
    )

    attempts: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0
    )

    failure_step: Mapped[str | None] = mapped_column(
        Text, nullable=True
    )

    error_output: Mapped[str | None] = mapped_column(
        Text, nullable=True
    )

    linked_indicator: Mapped[ISOIndicator | None] = mapped_column(
        Enum(ISOIndicator, name="isoindicator", create_type=False),
        nullable=True
    )

    spec: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False
    )

    extra: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)

    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(),
        nullable=False
    )

    evaluation = relationship("ExperimentEvaluatio", back_populates="probe_results")

    __table_args__ = (
        UniqueConstraint(
            "evaluation_id", "probe_name", name="uq_probe_results_evaluation_probe"
        ),
        Index("ix_probe_results_type_verdict", "probe_type", "verdict"),
    )
