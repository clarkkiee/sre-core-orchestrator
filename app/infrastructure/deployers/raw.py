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
        """Check that pods in namespace are running/ready."""
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
