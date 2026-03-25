import uuid

from app.models.chaos import ChaosExperiment, ChaosExperimentStatus
from app.models.job import Job, JobStatus, JobType
from app.repositories.chaos import ChaosRepository
from app.repositories.cluster import ClusterRepository
from app.repositories.deployment import DeploymentRepository
from app.repositories.job import JobRepository
from app.schemas.chaos import (
    ChaosExperimentListResponse,
    ChaosExperimentResponse,
    StartChaosExperimentRequest,
)
from app.tasks.chaos_tasks import run_chaos_experiment_task


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
        tenant_id: uuid.UUID,
        experiment_id: uuid.UUID,
    ) -> ChaosExperimentResponse | None:
        exp = await self.chaos_repository.get_by_id(experiment_id)
        if exp and exp.tenant_id == tenant_id:
            return ChaosExperimentResponse(**self._experiment_to_fields(exp))
        return None

    async def list_experiment(
        self,
        tenant_id: uuid.UUID,
        cluster_id: uuid.UUID | None,
    ) -> ChaosExperimentListResponse:
        experiments = await self.chaos_repository.list_by_tenant(
            tenant_id=tenant_id, cluster_id=cluster_id
        )
        items = [
            ChaosExperimentResponse(**self._experiment_to_fields(exp))
            for exp in experiments
        ]
        return ChaosExperimentListResponse(experiments=items, total=len(items))

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

        # TODO: Revoke celery task and delete ChaosEngine CR
        await self.chaos_repository.update(exp, status=ChaosExperimentStatus.STOPPED)
        return exp

    @staticmethod
    def _experiment_to_fields(experiment: ChaosExperiment) -> dict[str, object]:
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
