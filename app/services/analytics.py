from app.models.user import User
from app.repositories.campaign import CampaignRepository
from app.repositories.cluster import ClusterRepository
from app.repositories.chaos import ChaosRepository
from app.repositories.deployment import DeploymentRepository
from app.schemas.analytics import AnalyticResponse, EntityStats, StatusCount

class AnalyticsService:
    def __init__(
        self,
        cluster_repository: ClusterRepository,
        deployment_repository: DeploymentRepository,
        chaos_repository: ChaosRepository,
        campaign_repository: CampaignRepository
    ) -> None:
        self.cluster_repository = cluster_repository
        self.deployment_repository = deployment_repository
        self.chaos_repository = chaos_repository
        self.campaign_repository = campaign_repository
        
    async def get_stats(
        self,
        user: User
    ) -> AnalyticResponse:
        tenant_id = None if user.is_admin else user.id
        
        cluster_counts = await self.cluster_repository.count_by_status(tenant_id)
        deployment_counts = await self.deployment_repository.count_by_status(tenant_id)
        chaos_experiment_counts = await self.chaos_repository.count_by_status(tenant_id)
        campaign_counts = await self.campaign_repository.count_by_status(tenant_id)
        
        return AnalyticResponse(
            chaos_campaigns=self._to_entity_stats(campaign_counts),
            chaos_experiments=self._to_entity_stats(chaos_experiment_counts),
            clusters=self._to_entity_stats(cluster_counts),
            deployments=self._to_entity_stats(deployment_counts)
        )
        
    @staticmethod
    def _to_entity_stats(counts: dict) -> EntityStats:
        by_status = [
            StatusCount(count=count, status=status)
            for status, count in counts.items()
        ]
        return EntityStats(total=sum(counts.values()), by_status=by_status)
        