"""Helm chart deployer -- runs helm install/upgrade."""

import logging
from pathlib import Path

from app.infrastructure.deployers.base import BaseDeployer
from app.schemas.platform_config import HelmConfig

logger = logging.getLogger(__name__)


class HelmDeployer(BaseDeployer):
    strategy_config: HelmConfig

    async def deploy(self) -> str:
        chart_path = str(Path(self.repo_path) / self.strategy_config.chart_path)
        release_name = self.strategy_config.release_name or "app-release"

        cmd = [
            "helm",
            "upgrade",
            "--install",
            release_name,
            chart_path,
            "--namespace",
            self.namespace,
            "--create-namespace",
            "--wait",
            "--timeout",
            "300s",
        ]

        if self.strategy_config.values_path:
            values_path = str(
                Path(self.repo_path) / self.strategy_config.values_path,
            )
            cmd.extend(["-f", values_path])

        logger.info("Running helm install/upgrade for release '%s'", release_name)
        return await self._run_command(*cmd)

    async def verify(self) -> bool:
        release_name = self.strategy_config.release_name or "app-release"
        output = await self._run_command(
            "helm",
            "status",
            release_name,
            "--namespace",
            self.namespace,
            "-o",
            "json",
        )
        return "deployed" in output.lower()
