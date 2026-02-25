"""Cluster service — business logic for cluster provisioning."""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from app.exceptions.errors import ConflictError, NotFoundError
from app.infrastructure.kind_config import KindConfigBuilder
from app.models.cluster import Cluster, ClusterStatus
from app.models.job import Job, JobStatus, JobType
from app.repositories.cluster import ClusterRepository
from app.repositories.job import JobRepository
from app.schemas.cluster import (
    ClusterListResponse,
    ClusterResponse,
    ClusterWithJobResponse,
    CreateClusterRequest,
    DeleteClusterResponse,
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
            "kubeconfig_path": cluster.kubeconfig_path,
            "worker_count": ports.get("worker_count"),
            "created_at": cluster.created_at,
            "updated_at": cluster.updated_at,
            "expires_at": cluster.expires_at,
        }
