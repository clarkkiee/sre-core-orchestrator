import enum
import uuid
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from sqlalchemy import DateTime, Enum, ForeignKey, String, Text, func
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models import Base

if TYPE_CHECKING:
    from app.models.job import Job
    from app.models.user import User


class ClusterStatus(enum.StrEnum):
    PENDING = "pending"
    PROVISIONING = "provisioning"
    READY = "ready"
    FAILED = "failed"
    DELETING = "deleting"
    DELETED = "deleted"


class Cluster(Base):
    __tablename__ = "clusters"

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

    name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        comment="User-provided cluster name",
    )

    kind_name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        unique=True,
        index=True,
        comment="The kind cluster name",
    )

    app_preset: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True,
        comment="The application preset used for this cluster",
    )

    status: Mapped[ClusterStatus] = mapped_column(
        Enum(ClusterStatus),
        nullable=False,
        default=ClusterStatus.PENDING,
        index=True,
    )

    status_message: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="Additional information about the current status of the cluster",
    )

    kubeconfig_path: Mapped[str | None] = mapped_column(
        String(500),
        nullable=True,
    )

    kubeconfig: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="Kubeconfig YAML content stored directly in DB",
    )

    ports: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        nullable=True,
        comment="The cluster ports configuration",
    )

    victoriametrics_url: Mapped[str | None] = mapped_column(
        String(500),
        nullable=True,
        comment="The VictoriaMetrics URL for the cluster",
    )

    baseline_path: Mapped[str | None] = mapped_column(
        String(500),
        nullable=True,
        comment="The path to the baseline configuration for the cluster",
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

    expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        index=True,
        comment="The expiration time of the cluster (default: created_at + 7 days)",
    )

    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="The deletion time of the cluster",
    )

    # Relationships
    tenant: Mapped["User"] = relationship("User", back_populates="clusters")
    jobs: Mapped[list["Job"]] = relationship(
        "Job",
        back_populates="cluster",
        cascade="all, delete-orphan",
    )

    def __init__(self, **kwargs: Any) -> None:  # noqa: ANN401
        if "expires_at" not in kwargs:
            kwargs["expires_at"] = datetime.now(UTC) + timedelta(days=7)
        super().__init__(**kwargs)

    @property
    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return datetime.now(UTC) > self.expires_at

    def __repr__(self) -> str:
        return (
            f"<Cluster(id={self.id}, kind_name={self.kind_name}, status={self.status})>"
        )
