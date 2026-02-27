"""Kustomize deployer -- applies manifests via kubectl apply -k."""

import logging
from pathlib import Path

from app.infrastructure.deployers.base import BaseDeployer
from app.schemas.platform_config import KustomizeConfig

logger = logging.getLogger(__name__)


class KustomizeDeployer(BaseDeployer):
    strategy_config: KustomizeConfig

    async def deploy(self) -> str:
        kustomize_path = str(Path(self.repo_path) / self.strategy_config.path)
        logger.info("Applying kustomize from %s", kustomize_path)

        return await self._run_command(
            "kubectl",
            "apply",
            "-k",
            kustomize_path,
            "-n",
            self.namespace,
        )

    async def verify(self) -> bool:
        output = await self._run_command(
            "kubectl",
            "get",
            "pods",
            "-n",
            self.namespace,
            "-o",
            "jsonpath={.items[*].status.phase}",
        )
        if not output:
            return True
        phases = output.split()
        return all(p in ("Running", "Succeeded") for p in phases)
