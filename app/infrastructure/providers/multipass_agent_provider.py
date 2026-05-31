from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any

from app.infrastructure.agent.bootstrap import AgentBootstrap
from app.infrastructure.agent.client import AgentClient
from app.infrastructure.agent.exceptions import (
    AgentTaskFailedError,
    AgentUnreachableError,
)
from app.infrastructure.multipass.ssh_manager import SSHConfig
from app.infrastructure.providers.base import (
    ClusterProvider,
    ProgressCallback,
    ProvisionResult,
)
from app.infrastructure.config_values import load_cluster_profile
from app.utils.config import settings

logger = logging.getLogger(__name__)


class MultipassAgentProvider(ClusterProvider):
    def __init__(self) -> None:
        agent_host = settings.AGENT_HOST or settings.SSH_HOST
        self._base_url = f"http://{agent_host}:{settings.AGENT_PORT}"

        self._client = AgentClient(
            base_url=self._base_url,
            token=settings.AGENT_API_TOKEN,
        )
        self._bootstrap = AgentBootstrap(
            ssh_config=SSHConfig(
                host=settings.SSH_HOST,
                port=settings.SSH_PORT,
                username=settings.SSH_USER,
                private_key_path=settings.SSH_KEY_PATH,
                password=settings.SSH_PASSWORD,
            ),
            agent_binary_path=settings.AGENT_BINARY_PATH,
            agent_port=settings.AGENT_PORT,
            agent_token=settings.AGENT_API_TOKEN,
            agent_host=agent_host,
        )

    async def _ensure_agent_running(self) -> None:
        """Ensure the agent is running, bootstrapping if needed."""
        if await self._client.health():
            return
        logger.info("Agent not responding; triggering bootstrap")
        await self._bootstrap.ensure_running()

        # If bootstrap generated an ephemeral token, update the client.
        active_token = self._bootstrap.active_token
        if active_token and active_token != settings.AGENT_API_TOKEN:
            logger.info("Updating client with bootstrap-generated token")
            await self._client.close()
            self._client = AgentClient(
                base_url=self._base_url,
                token=active_token,
            )

    async def _poll_until_complete(
        self,
        task_id: str,
        on_progress: ProgressCallback | None = None,
    ) -> dict[str, Any]:
        """Poll a task until it reaches a terminal state."""
        poll_interval = settings.AGENT_POLL_INTERVAL
        timeout = settings.AGENT_POLL_TIMEOUT
        elapsed = 0.0

        while elapsed < timeout:
            task = await self._client.get_task(task_id)
            status = task.get("status", "")
            phase = task.get("phase", "")
            progress = task.get("progress", 0)

            if on_progress and phase:
                await on_progress(phase, progress)

            if status == "completed":
                return task
            if status == "failed":
                error_msg = task.get("error", "unknown error")
                raise AgentTaskFailedError(task_id, error_msg)
            if status == "cancelled":
                msg = f"Agent task {task_id} was cancelled"
                raise AgentTaskFailedError(task_id, msg)

            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

        msg = f"Agent task {task_id} timed out after {timeout}s"
        raise AgentTaskFailedError(task_id, msg)

    @staticmethod
    def _server_vm_name(cluster_name: str) -> str:
        return f"{cluster_name}-server"


    async def prepare_config(
        self,
        cluster_name: str,  # noqa: ARG002
        worker_count: int,
    ) -> dict[str, Any]:
        profile = load_cluster_profile()
        return {
            "provider": "multipass_k3s",
            "worker_count": worker_count,
            "server_profile": profile["server"],
            "worker_profile": profile["worker"],
        }

    async def provision(
        self,
        cluster_name: str,
        config: dict[str, Any],
        on_progress: ProgressCallback | None = None,
    ) -> ProvisionResult:
        """Provision cluster via the agent REST API."""
        await self._ensure_agent_running()

        task_id = str(uuid.uuid4())
        worker_count = config.get("worker_count", 2)

        profile = load_cluster_profile()
        sizing = config.get("worker_profile", profile["worker"])
        
        await self._client.submit_provision(
            task_id=task_id,
            cluster_name=cluster_name,
            worker_count=worker_count,
            vm_cpus=sizing["cpus"],
            vm_memory=sizing["memory"],
            vm_disk=sizing["disk"],
            disable_traefik=settings.K3S_DISABLE_TRAEFIK,
        )

        task = await self._poll_until_complete(task_id, on_progress)
        result = task.get("result", {})

        return ProvisionResult(
            kubeconfig_content=result.get("kubeconfig_content", ""),
            control_plane_ip=result.get("control_plane_ip", ""),
        )

    async def teardown(
        self,
        cluster_name: str,
        config: dict[str, Any],
    ) -> None:
        """Teardown cluster via the agent REST API."""
        await self._ensure_agent_running()

        task_id = str(uuid.uuid4())
        worker_count = config.get("worker_count", 2)

        await self._client.submit_teardown(
            task_id=task_id,
            cluster_name=cluster_name,
            worker_count=worker_count,
        )

        await self._poll_until_complete(task_id)

    async def reconnect(
        self,
        cluster_name: str,
        config: dict[str, Any],  # noqa: ARG002
    ) -> ProvisionResult:
        """Reconnect to existing cluster by querying the agent for VM info."""
        await self._ensure_agent_running()

        server_vm = self._server_vm_name(cluster_name)
        vm_info = await self._client.get_vm(server_vm)

        if vm_info is None:
            msg = (
                f"Server VM {server_vm!r} not found via agent. "
                "Please delete this cluster and re-provision."
            )
            raise RuntimeError(msg)

        server_ip = vm_info.get("ipv4", "")
        if not server_ip:
            msg = f"Server VM {server_vm!r} has no IP address"
            raise RuntimeError(msg)

        # Retrieve kubeconfig via a lightweight provision-like call.
        # For now, we use the VM IP and construct a reconnect task.
        # The agent's GET /vms/{name} gives us the IP; we need kubeconfig
        # from the server VM. Submit a special reconnect or just read it.
        # Since we can't exec into the VM from here, we submit a provision
        # task that the agent handles, but for reconnect we actually
        # need the agent to support this. For now, use a simple approach:
        # the orchestrator can use the SSH manager briefly to just get kubeconfig.

        # Use the existing SSH approach for the brief kubeconfig retrieval.
        from app.infrastructure.multipass.k3s_bootstrap import K3sBootstrapper
        from app.infrastructure.multipass.ssh_client import MultipassSSHClient
        from app.infrastructure.multipass.ssh_manager import SSHManager

        ssh_config = SSHConfig(
            host=settings.SSH_HOST,
            port=settings.SSH_PORT,
            username=settings.SSH_USER,
            private_key_path=settings.SSH_KEY_PATH,
            password=settings.SSH_PASSWORD,
        )
        ssh = SSHManager(ssh_config)
        async with ssh:
            mp = MultipassSSHClient(ssh, multipass_binary=settings.MULTIPASS_BINARY)
            k3s = K3sBootstrapper(mp)
            kubeconfig = await k3s.get_kubeconfig(server_vm, server_ip)

        return ProvisionResult(
            kubeconfig_content=kubeconfig,
            control_plane_ip=server_ip,
        )

    async def check_exists(
        self,
        cluster_name: str,
        config: dict[str, Any],  # noqa: ARG002
    ) -> bool:
        """Return True if the server VM exists (checked via agent)."""
        try:
            await self._ensure_agent_running()
        except (AgentUnreachableError, Exception):
            return False

        vm_info = await self._client.get_vm(self._server_vm_name(cluster_name))
        return vm_info is not None
