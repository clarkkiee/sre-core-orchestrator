import uuid

from sqlalchemy import inspect

from app.models.campaign import CampaignStatus, ChaosCampaign
from app.models.job import Job, JobStatus, JobType
from app.repositories.campaign import CampaignRepository
from app.repositories.cluster import ClusterRepository
from app.repositories.deployment import DeploymentRepository
from app.repositories.job import JobRepository
from app.schemas.campaign import (
    CampaignListResponse,
    CampaignResponse,
    StartCampaignRequest,
    StopCampaignResponse,
)
from app.schemas.chaos import ChaosExperimentResponse
from app.tasks.campaign_tasks import run_chaos_campaign_task


class CampaignService:
    def __init__(
        self,
        campaign_repository: CampaignRepository,
        cluster_repository: ClusterRepository,
        deployment_repository: DeploymentRepository,
        job_repository: JobRepository,
    ) -> None:
        self.campaign_repository = campaign_repository
        self.cluster_repository = cluster_repository
        self.deployment_repository = deployment_repository
        self.job_repository = job_repository

    async def start_campaign(
        self, tenant_id: uuid.UUID, payload: StartCampaignRequest
    ) -> tuple[CampaignResponse, uuid.UUID]:
        # Fetch deployment to derive target namespace
        deployment = await self.deployment_repository.get_by_id(payload.deployment_id)
        if not deployment:
            msg = f"Deployment {payload.deployment_id} not found"
            raise ValueError(msg)

        campaign = ChaosCampaign(
            tenant_id=tenant_id,
            cluster_id=payload.cluster_id,
            deployment_id=payload.deployment_id,
            target_namespace=deployment.namespace,
            status=CampaignStatus.PENDING,
        )
        campaign = await self.campaign_repository.create(campaign)

        job = Job(
            tenant_id=tenant_id,
            cluster_id=payload.cluster_id,
            deployment_id=payload.deployment_id,
            campaign_id=campaign.id,
            job_type=JobType.RUN_CHAOS_CAMPAIGN,
            status=JobStatus.PENDING,
        )
        job = await self.job_repository.create(job)

        await self.job_repository.db.commit()

        celery_result = run_chaos_campaign_task.delay(
            str(campaign.id), str(job.id)
        )
        await self.job_repository.update(job, celery_task_id=celery_result.id)

        return self._campaign_to_response(campaign), job.id

    async def get_campaign(
        self,
        tenant_id: uuid.UUID,
        campaign_id: uuid.UUID,
    ) -> CampaignResponse | None:
        campaign = await self.campaign_repository.get_by_id(campaign_id)
        if campaign and campaign.tenant_id == tenant_id:
            return self._campaign_to_response(campaign)
        return None

    async def list_campaigns(
        self,
        tenant_id: uuid.UUID,
        cluster_id: uuid.UUID | None = None,
    ) -> CampaignListResponse:
        campaigns = await self.campaign_repository.list_by_tenant(
            tenant_id=tenant_id, cluster_id=cluster_id
        )
        items = [self._campaign_to_response(c) for c in campaigns]
        return CampaignListResponse(campaigns=items, total=len(items))

    async def stop_campaign(
        self,
        tenant_id: uuid.UUID,
        campaign_id: uuid.UUID,
    ) -> StopCampaignResponse | None:
        campaign = await self.campaign_repository.get_by_id(campaign_id)
        if not campaign or campaign.tenant_id != tenant_id:
            return None

        if campaign.status not in (
            CampaignStatus.PENDING,
            CampaignStatus.DISCOVERING,
            CampaignStatus.RUNNING,
        ):
            return StopCampaignResponse(
                id=campaign.id,
                status=campaign.status,
                message="Campaign is not active",
            )

        await self.campaign_repository.update(
            campaign, status=CampaignStatus.STOPPED
        )
        await self.campaign_repository.db.commit()

        return StopCampaignResponse(
            id=campaign.id,
            status=CampaignStatus.STOPPED,
            message="Campaign stopped",
        )

    @staticmethod
    def _campaign_to_response(campaign: ChaosCampaign) -> CampaignResponse:
        # Avoid triggering async lazy-loads in sync response mapping.
        state = inspect(campaign)
        is_experiments_loaded = "experiments" not in state.unloaded
        campaign_experiments = campaign.experiments if is_experiments_loaded else []

        experiments = [
            ChaosExperimentResponse(
                id=e.id,
                tenant_id=e.tenant_id,
                cluster_id=e.cluster_id,
                deployment_id=e.deployment_id,
                experiment_type=e.experiment_type,
                target_namespace=e.target_namespace,
                target_label=e.target_label,
                status=e.status,
                status_message=e.status_message,
                duration_seconds=e.duration_seconds,
                chaos_engine_name=e.chaos_engine_name,
                configuration=e.configuration,
                result=e.result,
                started_at=e.started_at,
                completed_at=e.completed_at,
                created_at=e.created_at,
                updated_at=e.updated_at,
            )
            for e in campaign_experiments
        ]

        return CampaignResponse(
            id=campaign.id,
            tenant_id=campaign.tenant_id,
            cluster_id=campaign.cluster_id,
            deployment_id=campaign.deployment_id,
            target_namespace=campaign.target_namespace,
            status=campaign.status,
            status_message=campaign.status_message,
            discovered_services=campaign.discovered_services,
            total_experiments=campaign.total_experiments,
            completed_experiments=campaign.completed_experiments,
            experiments=experiments,
            started_at=campaign.started_at,
            completed_at=campaign.completed_at,
            created_at=campaign.created_at,
            updated_at=campaign.updated_at,
        )
