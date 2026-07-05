import uuid

from app.models.campaign import CampaignStatus, ChaosCampaign
from app.models.job import Job, JobStatus, JobType
from app.models.user import User
from app.repositories.campaign import CampaignRepository
from app.repositories.cluster import ClusterRepository
from app.repositories.deployment import DeploymentRepository
from app.repositories.job import JobRepository
from app.schemas.campaign import (
    CampaignFilter,
    CampaignPage,
    CampaignResponse,
    StartCampaignRequest,
    StopCampaignResponse,
)
from app.schemas.pagination import PaginationParams
from app.tasks.campaign_tasks import run_chaos_campaign_task
from app.exceptions.errors import NotFoundError


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
        user: User,
        campaign_id: uuid.UUID,
    ) -> CampaignResponse | None:
        campaign = await self.campaign_repository.get_by_id(campaign_id)
        if not campaign:
            msg = "Chaos campaign not found"
            raise NotFoundError(msg)
        if not user.is_admin and campaign.tenant_id != user.id:
            msg = "Cluster not found"
            raise NotFoundError(msg)
        return self._campaign_to_response(campaign)

    async def list_campaigns(
        self,
        user: User,
        params: PaginationParams,
        filters: CampaignFilter,
    ) -> CampaignPage:
        pagination = params.to_pagination()

        if user.is_admin:
            campaigns, total = await self.campaign_repository.list_all(
                pagination=pagination,
                filters=filters,
            )
        else:
            campaigns, total = await self.campaign_repository.list_by_tenant(
                user.id,
                pagination=pagination,
                filters=filters,
            )

        items = [self._campaign_to_response(c) for c in campaigns]
        return CampaignPage.create(items, total=total, params=params)

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
            started_at=campaign.started_at,
            completed_at=campaign.completed_at,
            created_at=campaign.created_at,
            updated_at=campaign.updated_at,
        )
