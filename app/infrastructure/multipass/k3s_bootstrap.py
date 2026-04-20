"""Bootstrap k3s server and agents on Multipass VMs."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

import yaml

from app.infrastructure.multipass.exceptions import K3sBootstrapError

if TYPE_CHECKING:
    from app.infrastructure.multipass.client import MultipassClient

logger = logging.getLogger(__name__)

_K3S_INSTALL_URL = "https://get.k3s.io"
_NODE_READY_POLL = 5


class K3sBootstrapper:
    """Installs k3s server + agents on Multipass VMs."""

    def __init__(self, multipass_client: MultipassClient) -> None:
        self._mp = multipass_client

    async def install_server(
        self,
        server_vm: str,
        *,
        disable_traefik: bool = True,
        tls_san: str | None = None,
        node_labels: dict[str, str] | None = None,
    ) -> tuple[str, str]:
        """Install k3s server on a VM.

        Returns:
            (node_token, kubeconfig_content)
        """
        exec_args = "server --write-kubeconfig-mode 644"
        if disable_traefik:
            exec_args += " --disable traefik"
        if tls_san:
            exec_args += f" --tls-san {tls_san}"
        if node_labels:
            for key, value in node_labels.items():
                exec_args += f" --node-label {key}={value}"

        install_cmd = (
            f"curl -sfL {_K3S_INSTALL_URL} "
            f'| INSTALL_K3S_EXEC="{exec_args}" sh -'
        )

        logger.info("Installing k3s server on VM %s", server_vm)
        await self._mp.exec_in_vm(server_vm, "bash", "-c", install_cmd)

        # Wait for k3s to be ready
        await self._wait_for_k3s_ready(server_vm)

        # Retrieve node token
        token = await self._mp.exec_in_vm(
            server_vm, "sudo", "cat", "/var/lib/rancher/k3s/server/node-token",
        )
        token = token.strip()

        # Retrieve kubeconfig
        kubeconfig = await self._mp.exec_in_vm(
            server_vm, "sudo", "cat", "/etc/rancher/k3s/k3s.yaml",
        )

        logger.info("k3s server installed on VM %s", server_vm)
        return token, kubeconfig

    async def join_agent(
        self,
        agent_vm: str,
        server_ip: str,
        token: str,
        *,
        node_labels: dict[str, str] | None = None,
    ) -> None:
        """Join a k3s agent (worker) to the server."""
        exec_args = "agent"
        if node_labels:
            for key, value in node_labels.items():
                exec_args += f" --node-label {key}={value}"

        install_cmd = (
            f"curl -sfL {_K3S_INSTALL_URL} "
            f"| K3S_URL=https://{server_ip}:6443 "
            f"K3S_TOKEN={token} "
            f'INSTALL_K3S_EXEC="{exec_args}" sh -'
        )

        logger.info("Joining agent VM %s to server %s", agent_vm, server_ip)
        await self._mp.exec_in_vm(agent_vm, "bash", "-c", install_cmd)
        logger.info("Agent VM %s joined successfully", agent_vm)

    async def get_kubeconfig(
        self,
        server_vm: str,
        server_ip: str,
    ) -> str:
        """Retrieve kubeconfig from server and rewrite 127.0.0.1 to server IP."""
        raw = await self._mp.exec_in_vm(
            server_vm, "sudo", "cat", "/etc/rancher/k3s/k3s.yaml",
        )
        kubeconfig = yaml.safe_load(raw)

        new_server = f"https://{server_ip}:6443"
        for cluster in kubeconfig.get("clusters", []):
            old_server = cluster.get("cluster", {}).get("server", "")
            cluster["cluster"]["server"] = new_server
            cluster["cluster"]["insecure-skip-tls-verify"] = True
            cluster["cluster"].pop("certificate-authority-data", None)
            logger.info("Rewrote kubeconfig server: %s -> %s", old_server, new_server)

        return yaml.safe_dump(kubeconfig, sort_keys=False)

    async def wait_for_nodes_ready(
        self,
        server_vm: str,
        expected_count: int,
        timeout_seconds: int = 180,
    ) -> None:
        """Wait until all k3s nodes report Ready."""
        elapsed = 0
        while elapsed < timeout_seconds:
            rc, stdout, _ = await self._mp.exec_in_vm_rc(
                server_vm,
                "sudo", "k3s", "kubectl", "get", "nodes",
                "--no-headers",
            )
            if rc == 0:
                lines = [l for l in stdout.splitlines() if l.strip()]  # noqa: E741
                ready_count = sum(1 for l in lines if " Ready " in l)  # noqa: E741
                if ready_count >= expected_count:
                    logger.info(
                        "All %d nodes ready on %s", expected_count, server_vm,
                    )
                    return

            await asyncio.sleep(_NODE_READY_POLL)
            elapsed += _NODE_READY_POLL

        msg = (
            f"Expected {expected_count} Ready nodes on {server_vm} "
            f"but timed out after {timeout_seconds}s"
        )
        raise K3sBootstrapError(msg)

    async def ensure_netem_module(self, vm_name: str) -> None:
        """Ensure sch_netem kernel module is loaded (required for LitmusChaos)."""
        await self._mp.exec_in_vm(vm_name, "sudo", "modprobe", "sch_netem")
        stdout = await self._mp.exec_in_vm(vm_name, "lsmod")
        if "sch_netem" not in stdout:
            msg = f"sch_netem module not loaded on VM {vm_name!r}"
            raise K3sBootstrapError(msg)
        logger.info("sch_netem module loaded on VM %s", vm_name)

    async def _wait_for_k3s_ready(
        self,
        vm_name: str,
        timeout_seconds: int = 120,
    ) -> None:
        """Wait until the k3s API server is responding."""
        elapsed = 0
        while elapsed < timeout_seconds:
            rc, _, _ = await self._mp.exec_in_vm_rc(
                vm_name,
                "sudo", "k3s", "kubectl", "get", "nodes",
            )
            if rc == 0:
                logger.info("k3s API server ready on VM %s", vm_name)
                return

            await asyncio.sleep(_NODE_READY_POLL)
            elapsed += _NODE_READY_POLL

        msg = f"k3s not ready on VM {vm_name!r} after {timeout_seconds}s"
        raise K3sBootstrapError(msg)
