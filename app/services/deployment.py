"""Deployment service -- business logic for application deployments."""

import uuid
from typing import Any

from app.exceptions.errors import ConflictError, NotFoundError
from app.models.cluster import ClusterStatus
from app.models.deployment import Deployment, DeploymentStatus, DeployStrategy
from app.models.job import Job, JobStatus, JobType
from app.models.user import User
from app.repositories.cluster import ClusterRepository
from app.repositories.deployment import DeploymentRepository
from app.repositories.job import JobRepository
from app.schemas.deployment import (
    AdminDeploymentPage,
    AdminDeploymentResponse,
    CreateDeploymentRequest,
    DeleteDeploymentResponse,
    DeploymentFilter,
    DeploymentPage,
    DeploymentResponse,
    DeploymentWithJobResponse,
)
from app.schemas.pagination import PaginationParams


class DeploymentService:
    def __init__(
        self,
        deployment_repository: DeploymentRepository,
        cluster_repository: ClusterRepository,
        job_repository: JobRepository,
    ) -> None:
        self.deployment_repository = deployment_repository
        self.cluster_repository = cluster_repository
        self.job_repository = job_repository

    async def create_deployment(
        self,
        user: User,
        payload: CreateDeploymentRequest,
    ) -> DeploymentWithJobResponse:
        # 1. Validate cluster exists, belongs to user (or user is admin), and is READY
        tenant_id = user.id
        cluster_id = uuid.UUID(payload.cluster_id)
        cluster = await self.cluster_repository.get_by_id(cluster_id)
        if not cluster:
            msg = "Cluster not found"
            raise NotFoundError(msg)
        if not user.is_admin and cluster.tenant_id != tenant_id:
            msg = "Cluster not found"
            raise NotFoundError(msg)
        if cluster.status != ClusterStatus.READY:
            msg = f"Cluster is not ready (status: {cluster.status.value})"
            raise ConflictError(msg)
        if not cluster.kubeconfig:
            msg = "Cluster kubeconfig is not available"
            raise ConflictError(msg)

        # 2. Create Deployment record (strategy updated after .platform.yaml parse)
        deployment = Deployment(
            tenant_id=tenant_id,
            cluster_id=cluster_id,
            repo_url=payload.repo_url,
            branch=payload.branch or "main",
            strategy=DeployStrategy.RAW,
            namespace=payload.namespace,
            status=DeploymentStatus.PENDING,
        )
        deployment = await self.deployment_repository.create(deployment)

        # 3. Create Job record
        job = Job(
            tenant_id=tenant_id,
            cluster_id=cluster_id,
            deployment_id=deployment.id,
            job_type=JobType.DEPLOY_APPLICATION,
            status=JobStatus.PENDING,
        )
        job = await self.job_repository.create(job)

        await self.job_repository.db.commit()

        # 4. Dispatch Celery task
        from app.tasks import deploy_application_task

        celery_result = deploy_application_task.delay(
            str(deployment.id),
            str(job.id),
            payload.github_token,
        )
        await self.job_repository.update(job, celery_task_id=celery_result.id)

        return DeploymentWithJobResponse(
            **self._to_fields(deployment),
            job_id=str(job.id),
            job_status=job.status.value,
        )

    async def get_deployment(
        self,
        user: User,
        deployment_id: uuid.UUID,
    ) -> DeploymentResponse | AdminDeploymentResponse:
        deployment = await self.deployment_repository.get_by_id(deployment_id)
        if not deployment:
            msg = "Deployment not found"
            raise NotFoundError(msg)
        if not user.is_admin and deployment.tenant_id != user.id:
            msg = "Deployment not found"
            raise NotFoundError(msg)
        if user.is_admin:
            return AdminDeploymentResponse(
                **self._to_fields(deployment),
                tenant_id=str(deployment.tenant_id),
            )
        return DeploymentResponse(**self._to_fields(deployment))

    async def delete_deployment(
        self,
        user: User,
        deployment_id: uuid.UUID,
    ) -> DeleteDeploymentResponse:
        deployment = await self.deployment_repository.get_by_id(deployment_id)
        if not deployment:
            msg = "Deployment not found"
            raise NotFoundError(msg)
        if not user.is_admin and deployment.tenant_id != user.id:
            msg = "Deployment not found"
            raise NotFoundError(msg)
        if deployment.status in (DeploymentStatus.DELETING, DeploymentStatus.DELETED):
            msg = "Deployment is already being deleted or has been deleted"
            raise ConflictError(msg)

        await self.deployment_repository.update(
            deployment,
            status=DeploymentStatus.DELETING,
        )

        job = Job(
            tenant_id=deployment.tenant_id,
            cluster_id=deployment.cluster_id,
            deployment_id=deployment.id,
            job_type=JobType.DELETE_DEPLOYMENT,
            status=JobStatus.PENDING,
        )
        job = await self.job_repository.create(job)

        await self.job_repository.db.commit()

        from app.tasks import delete_deployment_task

        celery_result = delete_deployment_task.delay(
            str(deployment.id),
            str(job.id),
        )
        await self.job_repository.update(job, celery_task_id=celery_result.id)

        return DeleteDeploymentResponse(
            id=str(deployment.id),
            status=DeploymentStatus.DELETING.value,
            job_id=str(job.id),
            message="Deployment deletion initiated",
        )

    async def list_deployments(
        self,
        user: User,
        params: PaginationParams,
        filters: DeploymentFilter,
    ) -> DeploymentPage | AdminDeploymentPage:
        pagination = params.to_pagination()

        if user.is_admin:
            deployments, total = await self.deployment_repository.list_all(
                pagination=pagination,
                filters=filters,
            )
            items = [
                AdminDeploymentResponse(
                    **self._to_fields(d),
                    tenant_id=str(d.tenant_id),
                )
                for d in deployments
            ]
            return AdminDeploymentPage.create(items, total=total, params=params)

        deployments, total = await self.deployment_repository.list_by_tenant(
            user.id,
            pagination=pagination,
            filters=filters,
        )
        items = [DeploymentResponse(**self._to_fields(d)) for d in deployments]
        return DeploymentPage.create(items, total=total, params=params)

    @staticmethod
    def _to_fields(deployment: Deployment) -> dict[str, Any]:
        return {
            "id": str(deployment.id),
            "cluster_id": str(deployment.cluster_id),
            "repo_url": deployment.repo_url,
            "branch": deployment.branch,
            "strategy": deployment.strategy.value,
            "namespace": deployment.namespace,
            "status": deployment.status.value,
            "status_message": deployment.status_message,
            "platform_config": deployment.platform_config,
            "created_at": deployment.created_at,
            "updated_at": deployment.updated_at,
            "completed_at": deployment.completed_at,
            "deleted_at": deployment.deleted_at,
        }
