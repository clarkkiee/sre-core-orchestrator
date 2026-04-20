"""Abstract base class for cluster infrastructure providers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

ProgressCallback = Callable[[str, int], Awaitable[None]]


@dataclass(frozen=True)
class ProvisionResult:
    """Result of a successful cluster provision or reconnect."""

    kubeconfig_content: str
    control_plane_ip: str


class ClusterProvider(ABC):
    """Abstract base for cluster infrastructure backends.

    Each provider owns phases 5-55% of provisioning progress.
    Shared post-provisioning steps (VERIFYING, monitoring, Linkerd, Litmus)
    are handled by cluster_tasks.py at 60-100%.
    """

    @abstractmethod
    async def prepare_config(
        self,
        cluster_name: str,
        worker_count: int,
    ) -> dict[str, Any]:
        """Return provider-specific config to persist in cluster.ports JSON.

        The returned dict MUST contain ``"provider"`` key with the
        provider identifier so teardown/reconnect can always instantiate
        the correct provider regardless of the current global setting.
        """

    @abstractmethod
    async def provision(
        self,
        cluster_name: str,
        config: dict[str, Any],
        on_progress: ProgressCallback | None = None,
    ) -> ProvisionResult:
        """Create cluster infrastructure and bootstrap Kubernetes.

        Call ``on_progress(phase_name, percentage)`` at each phase.
        Percentages must stay in the 5-55 range.
        """

    @abstractmethod
    async def teardown(
        self,
        cluster_name: str,
        config: dict[str, Any],
    ) -> None:
        """Destroy all cluster infrastructure."""

    @abstractmethod
    async def reconnect(
        self,
        cluster_name: str,
        config: dict[str, Any],
    ) -> ProvisionResult:
        """Reconnect to existing cluster, return fresh kubeconfig + IP.

        Raises ``RuntimeError`` if the underlying infrastructure no longer
        exists (container/VM deleted). The task layer handles this case.
        """

    @abstractmethod
    async def check_exists(
        self,
        cluster_name: str,
        config: dict[str, Any],
    ) -> bool:
        """Return True if the cluster infrastructure still exists."""
