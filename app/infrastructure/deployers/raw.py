"""Raw YAML manifest deployer -- applies manifests via kubectl apply -f."""

import logging
from pathlib import Path

from app.infrastructure.deployers.base import BaseDeployer
from app.schemas.platform_config import RawConfig

logger = logging.getLogger(__name__)


class RawDeployer(BaseDeployer):
    strategy_config: RawConfig

    async def deploy(self) -> str:
        manifest_path = str(Path(self.repo_path) / self.strategy_config.path)
        logger.info("Applying raw manifests from %s", manifest_path)

        return await self._run_command(
            "kubectl",
            "apply",
            "-f",
            manifest_path,
            "-n",
            self.namespace,
        )

    async def verify(self) -> bool:
        """Wait until all pods in the namespace are Ready."""
        logger.info("Waiting for all pods in %s to become Ready", self.namespace)
        await self._run_command(
            "kubectl",
            "wait",
            "--for=condition=Ready",
            "pods",
            "--all",
            "-n",
            self.namespace,
            "--timeout=300s",
        )
        logger.info("All pods in %s are Ready", self.namespace)
        return True
