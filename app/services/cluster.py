"""Cluster service — business logic for cluster provisioning."""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from app.exceptions.errors import ConflictError, NotFoundError
from app.infrastructure.kind import KindConfigBuilder
from app.models.cluster import Cluster, ClusterStatus
from app.models.job import Job, JobStatus, JobType
from app.repositories.cluster import ClusterRepository
from app.repositories.job import JobRepository
from app.schemas.cluster import (
    AdminClusterListResponse,
    AdminClusterResponse,
    ClusterHealthResponse,
    ClusterListResponse,
    ClusterResponse,
    ClusterWithJobResponse,
    CreateClusterRequest,
    DeleteClusterResponse,
    ReconnectClusterResponse,
)
from app.utils.config import settings


class ClusterService:
    def __init__(
        self,
        cluster_repository: ClusterRepository,
        job_repository: JobRepository,
    ) -> None:
        self.cluster_repository = cluster_repository
        self.job_repository = job_repository
        self.config_builder = KindConfigBuilder(
            port_range_start=settings.KIND_PORT_RANGE_START,
            port_range_end=settings.KIND_PORT_RANGE_END,
            ports_per_block=settings.KIND_PORTS_PER_BLOCK,
        )

    @staticmethod
    def _generate_kind_name(tenant_id: uuid.UUID, name: str) -> str:
        short_id = str(tenant_id)[:8]
        sanitized = name.lower().replace(" ", "-")[:20]
        return f"sre-{short_id}-{sanitized}"

    async def create_cluster(
        self,
        tenant_id: uuid.UUID,
        payload: CreateClusterRequest,
    ) -> ClusterWithJobResponse:
        kind_name = self._generate_kind_name(tenant_id, payload.name)

        # Check for name collision
        existing = await self.cluster_repository.get_by_kind_name(kind_name)
        if existing and existing.status not in (
            ClusterStatus.DELETED,
            ClusterStatus.FAILED,
        ):
            msg = f"Cluster '{payload.name}' already exists"
            raise ConflictError(msg)

        # Serialise port-block allocation
        await self.cluster_repository.acquire_port_allocation_lock()
        occupied = await self.cluster_repository.get_occupied_port_blocks()
        block_index = self.config_builder.find_available_block(
            kind_name,
            occupied,
        )
        ports_data = self.config_builder.allocate_ports(block_index)

        # Create cluster record
        cluster = Cluster(
            tenant_id=tenant_id,
            name=payload.name,
            kind_name=kind_name,
            app_preset=payload.app_preset,
            status=ClusterStatus.PENDING,
            ports={
                **ports_data,
                "worker_count": payload.worker_count,
                "allocated_at": datetime.now(UTC).isoformat(),
            },
            expires_at=datetime.now(UTC) + timedelta(days=payload.expires_in_days),
        )
        cluster = await self.cluster_repository.create(cluster)

        # Create provisioning job
        job = Job(
            tenant_id=tenant_id,
            cluster_id=cluster.id,
            job_type=JobType.PROVISION_CLUSTER,
            status=JobStatus.PENDING,
        )
        job = await self.job_repository.create(job)

        # Dispatch Celery task (imported here to avoid circular imports)
        from app.tasks import provision_cluster_task

        celery_result = provision_cluster_task.delay(
            str(cluster.id),
            str(job.id),
        )
        await self.job_repository.update(job, celery_task_id=celery_result.id)

        return ClusterWithJobResponse(
            **self._to_fields(cluster),
            job_id=str(job.id),
            job_status=job.status.value,
        )

    async def get_cluster(
        self,
        tenant_id: uuid.UUID,
        cluster_id: uuid.UUID,
    ) -> ClusterResponse:
        cluster = await self.cluster_repository.get_by_id(cluster_id)
        if not cluster or cluster.tenant_id != tenant_id:
            msg = "Cluster not found"
            raise NotFoundError(msg)
        return ClusterResponse(**self._to_fields(cluster))

    async def list_clusters(
        self,
        tenant_id: uuid.UUID,
    ) -> ClusterListResponse:
        clusters = await self.cluster_repository.list_by_tenant(tenant_id)
        items = [ClusterResponse(**self._to_fields(c)) for c in clusters]
        return ClusterListResponse(clusters=items, total=len(items))

    async def get_kubeconfig(
        self,
        tenant_id: uuid.UUID,
        cluster_id: uuid.UUID,
    ) -> str:
        cluster = await self.cluster_repository.get_by_id(cluster_id)
        if not cluster or cluster.tenant_id != tenant_id:
            msg = "Cluster not found"
            raise NotFoundError(msg)
        if not cluster.kubeconfig:
            msg = "Kubeconfig not available for this cluster"
            raise NotFoundError(msg)
        return cluster.kubeconfig

    async def delete_cluster(
        self,
        tenant_id: uuid.UUID,
        cluster_id: uuid.UUID,
    ) -> DeleteClusterResponse:
        cluster = await self.cluster_repository.get_by_id(cluster_id)
        if not cluster or cluster.tenant_id != tenant_id:
            msg = "Cluster not found"
            raise NotFoundError(msg)
        if cluster.status in (ClusterStatus.DELETING, ClusterStatus.DELETED):
            msg = "Cluster is already being deleted or has been deleted"
            raise ConflictError(msg)

        await self.cluster_repository.update(
            cluster,
            status=ClusterStatus.DELETING,
        )

        job = Job(
            tenant_id=tenant_id,
            cluster_id=cluster.id,
            job_type=JobType.TEARDOWN_CLUSTER,
            status=JobStatus.PENDING,
        )
        job = await self.job_repository.create(job)

        from app.tasks import teardown_cluster_task

        celery_result = teardown_cluster_task.delay(
            str(cluster.id),
            str(job.id),
        )
        await self.job_repository.update(job, celery_task_id=celery_result.id)

        return DeleteClusterResponse(
            id=str(cluster.id),
            status=ClusterStatus.DELETING.value,
            job_id=str(job.id),
            message="Cluster teardown initiated",
        )

    async def check_health(
        self,
        tenant_id: uuid.UUID,
        cluster_id: uuid.UUID,
    ) -> ClusterHealthResponse:
        """Check whether a cluster's Kubernetes API server is reachable."""
        cluster = await self.cluster_repository.get_by_id(cluster_id)
        if not cluster or cluster.tenant_id != tenant_id:
            msg = "Cluster not found"
            raise NotFoundError(msg)

        if cluster.status in (ClusterStatus.DELETING, ClusterStatus.DELETED):
            return ClusterHealthResponse(
                id=str(cluster.id),
                status=cluster.status.value,
                reachable=False,
                detail="Cluster has been deleted",
            )

        if not cluster.kubeconfig:
            return ClusterHealthResponse(
                id=str(cluster.id),
                status=cluster.status.value,
                reachable=False,
                detail="Cluster has no kubeconfig (not yet provisioned?)",
            )

        from app.infrastructure.kubernetes import ClusterHealthChecker

        checker = ClusterHealthChecker()
        reachable, detail = await checker.check_reachable(cluster.kubeconfig)

        # Update status based on health check result
        if not reachable and cluster.status == ClusterStatus.READY:
            await self.cluster_repository.update(
                cluster,
                status=ClusterStatus.UNREACHABLE,
                status_message=detail,
            )
        elif reachable and cluster.status == ClusterStatus.UNREACHABLE:
            await self.cluster_repository.update(
                cluster,
                status=ClusterStatus.READY,
                status_message="Cluster reachable again",
            )

        return ClusterHealthResponse(
            id=str(cluster.id),
            status=cluster.status.value,
            reachable=reachable,
            detail=detail,
        )

    async def _dispatch_reconnect(self, cluster: Cluster) -> ReconnectClusterResponse:
        """Create a reconnect job and dispatch the Celery task."""
        if cluster.status in (ClusterStatus.DELETING, ClusterStatus.DELETED):
            msg = "Cannot reconnect a deleted cluster"
            raise ConflictError(msg)
        if cluster.status == ClusterStatus.PROVISIONING:
            msg = "Cluster is still provisioning"
            raise ConflictError(msg)

        job = Job(
            tenant_id=cluster.tenant_id,
            cluster_id=cluster.id,
            job_type=JobType.RECONNECT_CLUSTER,
            status=JobStatus.PENDING,
        )
        job = await self.job_repository.create(job)

        from app.tasks import reconnect_cluster_task

        celery_result = reconnect_cluster_task.delay(
            str(cluster.id),
            str(job.id),
        )
        await self.job_repository.update(job, celery_task_id=celery_result.id)

        return ReconnectClusterResponse(
            id=str(cluster.id),
            status=cluster.status.value,
            job_id=str(job.id),
            message="Cluster reconnect initiated",
        )

    async def reconnect_cluster(
        self,
        tenant_id: uuid.UUID,
        cluster_id: uuid.UUID,
    ) -> ReconnectClusterResponse:
        """Reconnect a cluster (tenant-scoped)."""
        cluster = await self.cluster_repository.get_by_id(cluster_id)
        if not cluster or cluster.tenant_id != tenant_id:
            msg = "Cluster not found"
            raise NotFoundError(msg)
        return await self._dispatch_reconnect(cluster)

    async def admin_list_clusters(self) -> AdminClusterListResponse:
        """List all clusters across all tenants (admin only)."""
        clusters = await self.cluster_repository.list_all()
        items = [
            AdminClusterResponse(
                **self._to_fields(c),
                tenant_id=str(c.tenant_id),
            )
            for c in clusters
        ]
        return AdminClusterListResponse(clusters=items, total=len(items))

    async def admin_reconnect_cluster(
        self,
        cluster_id: uuid.UUID,
    ) -> ReconnectClusterResponse:
        """Reconnect any cluster regardless of tenant (admin only)."""
        cluster = await self.cluster_repository.get_by_id(cluster_id)
        if not cluster:
            msg = "Cluster not found"
            raise NotFoundError(msg)
        return await self._dispatch_reconnect(cluster)

    @staticmethod
    def _to_fields(cluster: Cluster) -> dict[str, Any]:
        ports = cluster.ports or {}
        return {
            "id": str(cluster.id),
            "name": cluster.name,
            "kind_name": cluster.kind_name,
            "status": cluster.status.value,
            "status_message": cluster.status_message,
            "app_preset": cluster.app_preset,
            "ports": cluster.ports,
            "has_kubeconfig": cluster.kubeconfig is not None,
            "worker_count": ports.get("worker_count"),
            "created_at": cluster.created_at,
            "updated_at": cluster.updated_at,
            "expires_at": cluster.expires_at,
        }
