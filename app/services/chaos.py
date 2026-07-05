import logging
import uuid
from typing import Any

from app.infrastructure.factories import build_litmus_manager
from app.models.chaos import ChaosExperiment, ChaosExperimentStatus
from app.models.job import Job, JobStatus, JobType
from app.models.user import User
from app.repositories.chaos import ChaosRepository
from app.repositories.cluster import ClusterRepository
from app.repositories.deployment import DeploymentRepository
from app.repositories.job import JobRepository
from app.schemas.chaos import (
    ChaosExperimentFilter,
    ChaosExperimentPage,
    ChaosExperimentResponse,
    StartChaosExperimentRequest,
)
from app.schemas.pagination import PaginationParams
from app.tasks.chaos_tasks import run_chaos_experiment_task
from app.exceptions.errors import NotFoundError

logger = logging.getLogger(__name__)

class ChaosService:
    def __init__(
        self,
        chaos_repository: ChaosRepository,
        cluster_repository: ClusterRepository,
        deployment_repository: DeploymentRepository,
        job_repository: JobRepository,
    ) -> None:
        self.chaos_repository = chaos_repository
        self.cluster_repository = cluster_repository
        self.deployment_repository = deployment_repository
        self.job_repository = job_repository

    async def start_experiment(
        self, tenant_id: uuid.UUID, payload: StartChaosExperimentRequest
    ) -> tuple[ChaosExperimentResponse, uuid.UUID]:
        # Create chaos experiment record
        engine_name = (
            f"{payload.experiment_type.value.lower().replace('_', '-')}"
            f"-{uuid.uuid4().hex[:8]}"
        )

        experiment = ChaosExperiment(
            tenant_id=tenant_id,
            cluster_id=payload.cluster_id,
            deployment_id=payload.deployment_id,
            experiment_type=payload.experiment_type,
            target_namespace=payload.target_namespace,
            target_label=payload.target_label,
            duration_seconds=payload.duration_seconds,
            chaos_engine_name=engine_name,
            configuration=payload.configuration,
            status=ChaosExperimentStatus.PENDING,
        )
        experiment = await self.chaos_repository.create(experiment)

        # Create Job
        job = Job(
            tenant_id=tenant_id,
            cluster_id=payload.cluster_id,
            deployment_id=payload.deployment_id,
            job_type=JobType.RUN_CHAOS_EXPERIMENT,
            status=JobStatus.PENDING,
        )
        job = await self.job_repository.create(job)

        await self.job_repository.db.commit()

        # Dispatch Celery task
        celery_result = run_chaos_experiment_task.delay(str(experiment.id), str(job.id))
        await self.job_repository.update(job, celery_task_id=celery_result.id)

        return ChaosExperimentResponse(**self._experiment_to_fields(experiment)), job.id

    async def get_experiment(
        self,
        user: User,
        experiment_id: uuid.UUID,
    ) -> ChaosExperimentResponse | None:
        exp = await self.chaos_repository.get_by_id(experiment_id)
        if not exp:
            msg = "Experiment not found"
            raise NotFoundError(msg)
        if not user.is_admin and exp.tenant_id != user.id:
            msg = "Cluster not found"
            raise NotFoundError(msg)
            
        return ChaosExperimentResponse(**self._experiment_to_fields(exp))
 

    async def list_experiment(
        self,
        user: User,
        params: PaginationParams,
        filters: ChaosExperimentFilter,
    ) -> ChaosExperimentPage:
        pagination = params.to_pagination()

        if user.is_admin:
            experiments, total = await self.chaos_repository.list_all(
                pagination=pagination,
                filters=filters,
            )
        else:
            experiments, total = await self.chaos_repository.list_by_tenant(
                user.id,
                pagination=pagination,
                filters=filters,
            )

        items = [
            ChaosExperimentResponse(**self._experiment_to_fields(exp))
            for exp in experiments
        ]
        return ChaosExperimentPage.create(items, total=total, params=params)

    async def stop_experiment(
        self,
        tenant_id: uuid.UUID,
        experiment_id: uuid.UUID,
    ) -> ChaosExperiment | None:
        exp = await self.chaos_repository.get_by_id(experiment_id)
        if not exp or exp.tenant_id != tenant_id:
            return None

        if exp.status not in (
            ChaosExperimentStatus.PENDING,
            ChaosExperimentStatus.RUNNING,
        ):
            return exp

        cluster = await self.cluster_repository.get_by_id(exp.cluster_id)
        if cluster and cluster.kubeconfig and exp.chaos_engine_name:
            try:
                manager = build_litmus_manager()
                await manager.delete_experiment(
                    cluster.kubeconfig,
                    exp.target_namespace,
                    exp.chaos_engine_name,
                )
            except Exception:
                logger.warning(
                    "Failed to delete ChaosEngine %s for experiment %s",
                    exp.chaos_engine_name, experiment_id, exc_info=True
                )

        await self.chaos_repository.update(exp, status=ChaosExperimentStatus.STOPPED)
        return exp

    @staticmethod
    def _experiment_to_fields(experiment: ChaosExperiment) -> dict[str, Any]:
        return {
            "id": experiment.id,
            "tenant_id": experiment.tenant_id,
            "cluster_id": experiment.cluster_id,
            "deployment_id": experiment.deployment_id,
            "experiment_type": experiment.experiment_type,
            "target_namespace": experiment.target_namespace,
            "target_label": experiment.target_label,
            "status": experiment.status,
            "status_message": experiment.status_message,
            "duration_seconds": experiment.duration_seconds,
            "chaos_engine_name": experiment.chaos_engine_name,
            "configuration": experiment.configuration,
            "result": experiment.result,
            "started_at": experiment.started_at,
            "completed_at": experiment.completed_at,
            "created_at": experiment.created_at,
            "updated_at": experiment.updated_at,
        }
