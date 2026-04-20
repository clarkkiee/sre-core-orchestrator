import enum
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import DateTime, Enum, ForeignKey, String, Text, func
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models import Base

if TYPE_CHECKING:
    from app.models.campaign import ChaosCampaign
    from app.models.chaos import ChaosExperiment
    from app.models.cluster import Cluster
    from app.models.job import Job
    from app.models.user import User


class DeploymentStatus(enum.StrEnum):
    PENDING = "PENDING"
    CLONING = "CLONING"
    VALIDATING = "VALIDATING"
    DEPLOYING = "DEPLOYING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    DELETING = "DELETING"
    DELETED = "DELETED"


class DeployStrategy(enum.StrEnum):
    RAW = "raw"
    HELM = "helm"
    SKAFFOLD = "skaffold"
    KUSTOMIZE = "kustomize"


class Deployment(Base):
    __tablename__ = "deployments"

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

    repo_url: Mapped[str] = mapped_column(
        String(500),
        nullable=False,
        comment="GitHub repository URL to deploy from",
    )

    branch: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        default="main",
        comment="Git branch to checkout",
    )

    strategy: Mapped[DeployStrategy] = mapped_column(
        Enum(
            DeployStrategy,
            values_callable=lambda e: [member.value for member in e],
        ),
        nullable=False,
        index=True,
        comment="Deployment strategy: raw, helm, skaffold, kustomize",
    )

    namespace: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        default="default",
        comment="Kubernetes namespace to deploy into",
    )

    platform_config: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        nullable=True,
        comment="Parsed .platform.yaml content",
    )

    status: Mapped[DeploymentStatus] = mapped_column(
        Enum(DeploymentStatus),
        nullable=False,
        default=DeploymentStatus.PENDING,
        index=True,
    )

    status_message: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="Additional information about the current deployment status",
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="The deletion time of the deployment",
    )

    # Relationships
    tenant: Mapped["User"] = relationship("User", back_populates="deployments")
    cluster: Mapped["Cluster"] = relationship("Cluster", back_populates="deployments")
    jobs: Mapped[list["Job"]] = relationship(
        "Job",
        back_populates="deployment",
        cascade="all, delete-orphan",
    )
    chaos_experiments: Mapped[list["ChaosExperiment"]] = relationship(
        "ChaosExperiment", back_populates="deployment", cascade="all, delete-orphan"
    )
    chaos_campaigns: Mapped[list["ChaosCampaign"]] = relationship(
        "ChaosCampaign", back_populates="deployment", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return (
            f"<Deployment(id={self.id}, repo_url={self.repo_url}, "
            f"strategy={self.strategy}, status={self.status})>"
        )
