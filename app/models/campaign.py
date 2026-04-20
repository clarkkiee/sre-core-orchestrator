"""Chaos Campaign Model"""

import enum
import uuid
from typing import TYPE_CHECKING, Any

from sqlalchemy import DateTime, Enum, ForeignKey, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models import Base

if TYPE_CHECKING:
    from app.models.chaos import ChaosExperiment
    from app.models.cluster import Cluster
    from app.models.deployment import Deployment
    from app.models.job import Job
    from app.models.user import User


class CampaignStatus(enum.StrEnum):
    PENDING = "PENDING"
    DISCOVERING = "DISCOVERING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    STOPPED = "STOPPED"


class ChaosCampaign(Base):
    __tablename__ = "chaos_campaigns"

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
    )

    target_namespace: Mapped[str] = mapped_column(String(253), nullable=False)

    status: Mapped[CampaignStatus] = mapped_column(
        Enum(CampaignStatus),
        nullable=False,
        default=CampaignStatus.PENDING,
    )

    status_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    discovered_services: Mapped[list[dict[str, Any]] | None] = mapped_column(
        JSON, nullable=True
    )

    total_experiments: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    completed_experiments: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0
    )

    started_at: Mapped[Any] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    completed_at: Mapped[Any] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    created_at: Mapped[Any] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    updated_at: Mapped[Any] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    tenant: Mapped["User"] = relationship("User", back_populates="chaos_campaigns")
    cluster: Mapped["Cluster"] = relationship(
        "Cluster", back_populates="chaos_campaigns"
    )
    deployment: Mapped["Deployment"] = relationship(
        "Deployment", back_populates="chaos_campaigns"
    )
    experiments: Mapped[list["ChaosExperiment"]] = relationship(
        "ChaosExperiment", back_populates="campaign", cascade="all, delete-orphan"
    )
    jobs: Mapped[list["Job"]] = relationship(
        "Job", back_populates="campaign", cascade="all, delete-orphan"
    )
