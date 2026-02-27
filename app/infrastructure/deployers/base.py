"""Abstract base class for all deployment strategies."""

import abc
import asyncio
import logging
import os
from typing import Any

from app.infrastructure.exceptions import DeployerCommandError

logger = logging.getLogger(__name__)


class BaseDeployer(abc.ABC):
    """Base class all deployers must inherit from."""

    def __init__(
        self,
        kubeconfig_path: str,
        repo_path: str,
        namespace: str,
        strategy_config: Any,  # noqa: ANN401
    ) -> None:
        self.kubeconfig_path = kubeconfig_path
        self.repo_path = repo_path
        self.namespace = namespace
        self.strategy_config = strategy_config

    def _build_env(self) -> dict[str, str]:
        """Build environment with KUBECONFIG set."""
        env = os.environ.copy()
        env["KUBECONFIG"] = self.kubeconfig_path
        path = env.get("PATH", "")
        for p in ("/usr/local/bin", "/usr/bin", "/bin"):
            if p not in path:
                path = f"{p}:{path}"
        env["PATH"] = path
        return env

    async def _run_command(self, *args: str) -> str:
        """Execute a CLI command and return stdout. Raises on failure."""
        cmd = list(args)
        logger.info("Running: %s", " ".join(cmd))

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=self._build_env(),
            cwd=self.repo_path,
        )
        stdout, stderr = await proc.communicate()
        stdout_text = stdout.decode().strip()
        stderr_text = stderr.decode().strip()

        if proc.returncode != 0:
            raise DeployerCommandError(
                command=" ".join(cmd),
                returncode=proc.returncode or 1,
                stderr=stderr_text,
            )

        if stderr_text:
            logger.debug("stderr: %s", stderr_text)

        return stdout_text

    async def ensure_namespace(self) -> None:
        """Create the target namespace if it does not exist."""
        try:
            await self._run_command(
                "kubectl",
                "get",
                "namespace",
                self.namespace,
            )
        except DeployerCommandError:
            logger.info("Creating namespace '%s'", self.namespace)
            await self._run_command(
                "kubectl",
                "create",
                "namespace",
                self.namespace,
            )

    @abc.abstractmethod
    async def deploy(self) -> str:
        """Execute the deployment. Returns a summary string."""
        ...

    @abc.abstractmethod
    async def verify(self) -> bool:
        """Verify the deployment succeeded."""
        ...
