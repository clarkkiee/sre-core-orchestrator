"""Cluster repository — database operations for cluster management."""

import uuid
from datetime import UTC, datetime

from sqlalchemy import and_, select, text

from app.models.cluster import Cluster, ClusterStatus
from app.repositories.base import BaseRepository


class ClusterRepository(BaseRepository[Cluster]):

    async def get_by_id(self, cluster_id: uuid.UUID) -> Cluster | None:
        result = await self.db.execute(
            select(Cluster).where(Cluster.id == cluster_id),
        )
        return result.scalar_one_or_none()

    async def get_by_kind_name(self, kind_name: str) -> Cluster | None:
        result = await self.db.execute(
            select(Cluster).where(Cluster.kind_name == kind_name),
        )
        return result.scalar_one_or_none()

    async def list_by_tenant(
        self,
        tenant_id: uuid.UUID,
        *,
        include_deleted: bool = False,
    ) -> list[Cluster]:
        stmt = select(Cluster).where(Cluster.tenant_id == tenant_id)
        if not include_deleted:
            stmt = stmt.where(Cluster.status != ClusterStatus.DELETED)
        stmt = stmt.order_by(Cluster.created_at.desc())
        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    async def list_all(
        self,
        *,
        include_deleted: bool = False,
    ) -> list[Cluster]:
        """Return all clusters across all tenants."""
        stmt = select(Cluster)
        if not include_deleted:
            stmt = stmt.where(Cluster.status != ClusterStatus.DELETED)
        stmt = stmt.order_by(Cluster.created_at.desc())
        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    async def acquire_port_allocation_lock(self) -> None:
        """Acquire a PostgreSQL advisory lock for port-block allocation.

        This serialises concurrent port allocation without table-level locks.
        The lock is automatically released when the transaction ends.
        """
        await self.db.execute(text("SELECT pg_advisory_xact_lock(1)"))

    async def get_occupied_port_blocks(self) -> set[int]:
        """Return the set of port block indices used by active clusters."""
        stmt = select(Cluster.ports).where(
            Cluster.status.notin_([ClusterStatus.DELETED, ClusterStatus.FAILED]),
        )
        result = await self.db.execute(stmt)
        blocks: set[int] = set()
        for (ports_json,) in result:
            if isinstance(ports_json, dict) and "block_index" in ports_json:
                blocks.add(ports_json["block_index"])
        return blocks

    async def get_expired_clusters(self) -> list[Cluster]:
        """Find clusters past their expiration that are still active."""
        stmt = select(Cluster).where(
            and_(
                Cluster.expires_at <= datetime.now(UTC),
                Cluster.status.notin_(
                    [ClusterStatus.DELETED, ClusterStatus.DELETING],
                ),
            ),
        )
        result = await self.db.execute(stmt)
        return list(result.scalars().all())
