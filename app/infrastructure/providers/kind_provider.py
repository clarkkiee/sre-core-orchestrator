"""Kind cluster provider — wraps KindClient and KindConfigBuilder."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from app.infrastructure.kind.client import KindClient
from app.infrastructure.kind.config_builder import KindConfigBuilder
from app.infrastructure.kind.exceptions import KindCommandError
from app.infrastructure.providers.base import (
    ClusterProvider,
    ProgressCallback,
    ProvisionResult,
)
from app.utils.config import settings

if TYPE_CHECKING:
    from app.repositories.cluster import ClusterRepository

logger = logging.getLogger(__name__)


class KindProvider(ClusterProvider):
    """Cluster provider backed by Kind (Kubernetes in Docker)."""

    def __init__(self, cluster_repo: ClusterRepository | None = None) -> None:
        self._kind_client = KindClient(kind_binary=settings.KIND_BINARY)
        self._config_builder = KindConfigBuilder(
            port_range_start=settings.KIND_PORT_RANGE_START,
            port_range_end=settings.KIND_PORT_RANGE_END,
            ports_per_block=settings.KIND_PORTS_PER_BLOCK,
        )
        self._cluster_repo = cluster_repo

    async def prepare_config(
        self,
        cluster_name: str,
        worker_count: int,
    ) -> dict[str, Any]:
        """Allocate a port block and return Kind-specific config."""
        if self._cluster_repo is not None:
            await self._cluster_repo.acquire_port_allocation_lock()
            occupied = await self._cluster_repo.get_occupied_port_blocks()
        else:
            occupied = set()

        block_index = self._config_builder.find_available_block(cluster_name, occupied)
        ports_data = self._config_builder.allocate_ports(block_index)
        return {
            **ports_data,
            "worker_count": worker_count,
            "provider": "kind",
        }

    async def provision(
        self,
        cluster_name: str,
        config: dict[str, Any],
        on_progress: ProgressCallback | None = None,
    ) -> ProvisionResult:
        """Build Kind config, create cluster, export and rewrite kubeconfig."""
        worker_count = config.get("worker_count", 2)

        # Phase: BUILDING_CONFIG (10%)
        if on_progress:
            await on_progress("BUILDING_CONFIG", 10)

        kind_config = self._config_builder.build_config(
            cluster_name=cluster_name,
            ports_data=config,
            worker_count=worker_count,
            registry_url=settings.PRIVATE_REGISTRY_URL,
        )
        config_path = self._config_builder.write_config(
            kind_config,
            Path(settings.KUBECONFIG_DIR) / f"kind-config-{cluster_name}.yaml",
        )

        # Phase: CREATING_CLUSTER (25%)
        if on_progress:
            await on_progress("CREATING_CLUSTER", 25)

        if not await self._kind_client.cluster_exists(cluster_name):
            await self._kind_client.create_cluster(cluster_name, str(config_path))

        # Phase: EXPORTING_KUBECONFIG (45%)
        if on_progress:
            await on_progress("EXPORTING_KUBECONFIG", 45)

        kubeconfig_path = str(
            Path(settings.KUBECONFIG_DIR) / f"kubeconfig-{cluster_name}.yaml",
        )
        await self._kind_client.export_kubeconfig(cluster_name, kubeconfig_path)

        network = await self._kind_client.get_own_network()
        await self._kind_client.connect_to_network(cluster_name, network)
        cp_ip = await self._kind_client.get_control_plane_ip(
            cluster_name, network=network
        )
        kubeconfig_content = await self._kind_client.rewrite_kubeconfig_server(
            kubeconfig_path,
            cp_ip,
        )

        return ProvisionResult(
            kubeconfig_content=kubeconfig_content,
            control_plane_ip=cp_ip,
        )

    async def teardown(
        self,
        cluster_name: str,
        config: dict[str, Any],  # noqa: ARG002
    ) -> None:
        """Delete the Kind cluster if it exists."""
        if await self._kind_client.cluster_exists(cluster_name):
            await self._kind_client.delete_cluster(cluster_name)

    async def reconnect(
        self,
        cluster_name: str,
        config: dict[str, Any],  # noqa: ARG002
    ) -> ProvisionResult:
        """Re-discover control-plane IP and export a fresh kubeconfig.

        Raises RuntimeError if the Kind container no longer exists.
        """
        container_name = f"{cluster_name}-control-plane"
        try:
            await self._kind_client._docker("inspect", container_name)  # noqa: SLF001
        except KindCommandError as exc:
            msg = (
                f"Kind container {container_name!r} not found. "
                "Please delete this cluster and re-provision."
            )
            raise RuntimeError(msg) from exc

        network = await self._kind_client.get_own_network()
        try:
            await self._kind_client.connect_to_network(cluster_name, network)
        except KindCommandError as exc:
            if "already exists" not in exc.stderr.lower():
                raise

        kubeconfig_path = str(
            Path(settings.KUBECONFIG_DIR) / f"kubeconfig-{cluster_name}.yaml",
        )
        await self._kind_client.export_kubeconfig(cluster_name, kubeconfig_path)
        cp_ip = await self._kind_client.get_control_plane_ip(
            cluster_name, network=network
        )
        kubeconfig_content = await self._kind_client.rewrite_kubeconfig_server(
            kubeconfig_path,
            cp_ip,
        )

        return ProvisionResult(
            kubeconfig_content=kubeconfig_content,
            control_plane_ip=cp_ip,
        )

    async def check_exists(
        self,
        cluster_name: str,
        config: dict[str, Any],  # noqa: ARG002
    ) -> bool:
        """Return True if the Kind cluster exists."""
        return await self._kind_client.cluster_exists(cluster_name)
