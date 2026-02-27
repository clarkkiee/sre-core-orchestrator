"""Skaffold deployer -- runs skaffold deploy."""

import logging
from pathlib import Path

from app.infrastructure.deployers.base import BaseDeployer
from app.schemas.platform_config import SkaffoldConfig

logger = logging.getLogger(__name__)


class SkaffoldDeployer(BaseDeployer):
    strategy_config: SkaffoldConfig

    async def deploy(self) -> str:
        config_path = str(Path(self.repo_path) / self.strategy_config.config_path)
        logger.info("Running skaffold deploy with config %s", config_path)

        return await self._run_command(
            "skaffold",
            "deploy",
            "-f",
            config_path,
            "--namespace",
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
