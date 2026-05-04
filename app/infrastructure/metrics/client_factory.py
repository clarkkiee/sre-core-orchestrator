import uuid

from app.exceptions.errors import ConflictError, NotFoundError
from app.infrastructure.metrics.client import VictoriaMetricsClient
from app.repositories.chaos import ChaosRepository
from app.repositories.cluster import ClusterRepository


class VictoriaMetricsClientFactory:
    def __init__(
        self,
        cluster_repo: ClusterRepository,
        experiment_repo: ChaosRepository
    ) -> None:
        self._cluster_repo = cluster_repo
        self._experiment_repo = experiment_repo

    async def for_experiment(
        self, experiment_id: uuid.UUID
    ) -> VictoriaMetricsClient:
        experiment = await self._experiment_repo.get_by_id(experiment_id)
        if experiment is None:
            msg = f"Experiment {experiment_id} not found"
            raise NotFoundError(msg)

        cluster = await self._cluster_repo.get_by_id(experiment.cluster_id)
        if cluster is None or not cluster.victoriametrics_url:
            msg = f"Cluster {experiment.cluster_id} has no VictoriaMetrics URL"
            raise ConflictError(msg)

        return VictoriaMetricsClient(cluster.victoriametrics_url)

    async def for_cluster(
        self, cluster_id: uuid.UUID
    ) -> VictoriaMetricsClient:
        cluster = await self._cluster_repo.get_by_id(cluster_id)
        if cluster is None:
            msg = f"Cluster {cluster_id} not found"
            raise NotFoundError(msg)
        if not cluster.victoriametrics_url:
            msg = f"Cluster {cluster_id} has no VictoriaMetrics URL"
            raise ConflictError(msg)

        return VictoriaMetricsClient(cluster.victoriametrics_url)
