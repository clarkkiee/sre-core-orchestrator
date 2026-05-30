from __future__ import annotations

import asyncio
import logging
from typing import Any

from app.infrastructure.multipass.cloud_init import CloudInitBuilder
from app.infrastructure.multipass.exceptions import MultipassVMNotFoundError
from app.infrastructure.multipass.k3s_bootstrap import K3sBootstrapper
from app.infrastructure.multipass.ssh_client import MultipassSSHClient
from app.infrastructure.multipass.ssh_manager import SSHConfig, SSHManager
from app.infrastructure.providers.base import (
    ClusterProvider,
    ProgressCallback,
    ProvisionResult,
)
from app.utils.config import settings
from app.infrastructure.config_values import load_cluster_profile

logger = logging.getLogger(__name__)


class MultipassK3sSSHProvider(ClusterProvider):

    def __init__(self) -> None:
        self._ssh_config = SSHConfig(
            host=settings.SSH_HOST,
            port=settings.SSH_PORT,
            username=settings.SSH_USER,
            private_key_path=settings.SSH_KEY_PATH,
            password=settings.SSH_PASSWORD,
        )
        self._cloud_init = CloudInitBuilder()

    def _build_ssh_manager(self) -> SSHManager:
        """Create a fresh SSHManager (one per operation)."""
        return SSHManager(self._ssh_config)

    def _build_client(self, ssh: SSHManager) -> MultipassSSHClient:
        return MultipassSSHClient(ssh, multipass_binary=settings.MULTIPASS_BINARY)

    @staticmethod
    def _server_vm_name(cluster_name: str) -> str:
        return f"{cluster_name}-server"

    @staticmethod
    def _worker_vm_name(cluster_name: str, index: int) -> str:
        return f"{cluster_name}-w{index}"

    def _all_vm_names(
        self, cluster_name: str, worker_count: int,
    ) -> list[str]:
        names = [self._server_vm_name(cluster_name)]
        for i in range(1, worker_count + 1):
            names.append(self._worker_vm_name(cluster_name, i))
        return names

    # ---- ClusterProvider interface ----

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
        
        profile = load_cluster_profile()
        worker_count = config.get("worker_count", 2)
        server = config.get("server_profile", profile["server"])        
        worker = config.get("worker_profile", profile["worker"])        


        server_vm = self._server_vm_name(cluster_name)

        ssh = self._build_ssh_manager()
        async with ssh:
            mp = self._build_client(ssh)
            k3s = K3sBootstrapper(mp) # type: ignore

            # Phase: BUILDING_CONFIG (5%)
            if on_progress:
                await on_progress("BUILDING_CONFIG", 5)

            server_ci = self._cloud_init.build_server_config()
            agent_ci = self._cloud_init.build_agent_config()

            # Stage cloud-init files on the remote host
            server_ci_path = await ssh.write_remote_tempfile(
                server_ci, suffix="-cloud-init.yaml", prefix="mp-server-",
            )
            agent_ci_path = await ssh.write_remote_tempfile(
                agent_ci, suffix="-cloud-init.yaml", prefix="mp-agent-",
            )

            try:
                # Phase: CREATING_SERVER_VM (15%)
                if on_progress:
                    await on_progress("CREATING_SERVER_VM", 15)

                await mp.launch_vm(
                    server_vm,
                    cpus=server["cpus"],
                    memory=server["memory"],
                    disk=server["disk"],
                    cloud_init_file=server_ci_path,
                )
                await mp.wait_for_cloud_init(server_vm)

                server_ip = await mp.get_vm_ip(server_vm)

                # Phase: BOOTSTRAPPING_K3S (30%)
                if on_progress:
                    await on_progress("BOOTSTRAPPING_K3S", 30)

                token, _ = await k3s.install_server(
                    server_vm,
                    disable_traefik=settings.K3S_DISABLE_TRAEFIK,
                    tls_san=server_ip,
                    node_labels={"ingress-ready": "true", "master": "true"},
                )

                # Phase: JOINING_WORKERS (45%)
                if on_progress:
                    await on_progress("JOINING_WORKERS", 45)

                join_tasks = []
                for i in range(1, worker_count + 1):
                    worker_vm = self._worker_vm_name(cluster_name, i)
                    join_tasks.append(
                        self._launch_and_join_worker(
                            mp, k3s, worker_vm, server_ip, token,
                            cpus=worker["cpus"], memory=worker["memory"], disk=worker["disk"],
                            cloud_init_file=agent_ci_path,
                        ),
                    )
                await asyncio.gather(*join_tasks)

                # Phase: EXPORTING_KUBECONFIG (55%)
                if on_progress:
                    await on_progress("EXPORTING_KUBECONFIG", 55)

                expected_nodes = 1 + worker_count
                await k3s.wait_for_nodes_ready(server_vm, expected_nodes)
                kubeconfig = await k3s.get_kubeconfig(server_vm, server_ip)

                # Ensure kernel modules for LitmusChaos on all VMs
                for vm in self._all_vm_names(cluster_name, worker_count):
                    await k3s.ensure_netem_module(vm)

            finally:
                # Clean up remote temp cloud-init files
                await ssh.remove_remote_file(server_ci_path)
                await ssh.remove_remote_file(agent_ci_path)

        return ProvisionResult(
            kubeconfig_content=kubeconfig,
            control_plane_ip=server_ip,
        )

    async def teardown(
        self,
        cluster_name: str,
        config: dict[str, Any],
    ) -> None:
        """Delete server and all worker VMs via SSH."""
        worker_count = config.get("worker_count", 2)

        ssh = self._build_ssh_manager()
        async with ssh:
            mp = self._build_client(ssh)

            for vm in self._all_vm_names(cluster_name, worker_count):
                if await mp.vm_exists(vm):
                    await mp.delete_vm(vm)
                    logger.info("Deleted VM %s (via SSH)", vm)

    async def reconnect(
        self,
        cluster_name: str,
        config: dict[str, Any],  # noqa: ARG002
    ) -> ProvisionResult:
        """Reconnect to existing Multipass+k3s cluster via SSH.

        Raises RuntimeError if the server VM no longer exists.
        """
        server_vm = self._server_vm_name(cluster_name)

        ssh = self._build_ssh_manager()
        async with ssh:
            mp = self._build_client(ssh)
            k3s = K3sBootstrapper(mp) # type: ignore

            try:
                server_ip = await mp.get_vm_ip(server_vm)
            except MultipassVMNotFoundError as exc:
                msg = (
                    f"Server VM {server_vm!r} not found. "
                    "Please delete this cluster and re-provision."
                )
                raise RuntimeError(msg) from exc

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
        """Return True if the server VM exists (checked via SSH)."""
        ssh = self._build_ssh_manager()
        async with ssh:
            mp = self._build_client(ssh)
            return await mp.vm_exists(self._server_vm_name(cluster_name))

    # ---- private helpers ----

    @staticmethod
    async def _launch_and_join_worker(  # noqa: PLR0913
        mp: MultipassSSHClient,
        k3s: K3sBootstrapper,
        worker_vm: str,
        server_ip: str,
        token: str,
        *,
        cpus: int,
        memory: str,
        disk: str,
        cloud_init_file: str,
    ) -> None:
        """Launch a worker VM, wait for cloud-init, then join it to k3s."""
        await mp.launch_vm(
            worker_vm,
            cpus=cpus,
            memory=memory,
            disk=disk,
            cloud_init_file=cloud_init_file,
        )
        await mp.wait_for_cloud_init(worker_vm)
        await k3s.join_agent(
            worker_vm,
            server_ip,
            token,
            node_labels={"worker": "true"},
        )
