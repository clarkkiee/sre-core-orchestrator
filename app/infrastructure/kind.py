"""Async wrapper around the Kind CLI binary."""

import asyncio
import logging
import os
import socket
from pathlib import Path

import yaml

from app.infrastructure.exceptions import KindCommandError
from app.utils.config import settings

logger = logging.getLogger(__name__)


class KindClient:
    """Low-level wrapper around the ``kind`` CLI."""

    def __init__(self, kind_binary: str = "kind") -> None:
        self.kind_binary = kind_binary

    def _build_env(self) -> dict[str, str]:
        """Build an environment dict with corrected PATH and DOCKER_HOST."""
        env = os.environ.copy()
        path = env.get("PATH", "")
        for p in ("/usr/local/bin", "/usr/bin", "/bin"):
            if p not in path:
                path = f"{p}:{path}"
        env["PATH"] = path
        if settings.DOCKER_HOST:
            env["DOCKER_HOST"] = settings.DOCKER_HOST
        return env

    async def _run(self, *args: str) -> str:
        cmd = [self.kind_binary, *args]
        logger.info("Running: %s", " ".join(cmd))

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=self._build_env(),
        )
        stdout, stderr = await proc.communicate()

        stdout_text = stdout.decode().strip()
        stderr_text = stderr.decode().strip()

        if proc.returncode != 0:
            raise KindCommandError(
                command=" ".join(cmd),
                returncode=proc.returncode or 1,
                stderr=stderr_text,
            )

        if stderr_text:
            logger.debug("kind stderr: %s", stderr_text)

        return stdout_text

    async def _docker(self, *args: str) -> str:
        """Run a docker CLI command and return stdout."""
        cmd = ["docker", *args]
        logger.info("Running: %s", " ".join(cmd))

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=self._build_env(),
        )
        stdout, stderr = await proc.communicate()

        if proc.returncode != 0:
            raise KindCommandError(
                command=" ".join(cmd),
                returncode=proc.returncode or 1,
                stderr=stderr.decode().strip(),
            )

        return stdout.decode().strip()

    # ---- Kind cluster operations ----

    async def create_cluster(self, name: str, config_path: str | Path) -> None:
        logger.info("Creating Kind cluster '%s'", name)
        await self._run(
            "create",
            "cluster",
            "--name",
            name,
            "--config",
            str(config_path),
        )
        logger.info("Kind cluster '%s' created successfully", name)

    async def delete_cluster(self, name: str) -> None:
        logger.info("Deleting Kind cluster '%s'", name)
        await self._run("delete", "cluster", "--name", name)
        logger.info("Kind cluster '%s' deleted", name)

    async def export_kubeconfig(
        self,
        name: str,
        output_path: str | Path,
    ) -> str:
        output = str(output_path)
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        await self._run(
            "export",
            "kubeconfig",
            "--name",
            name,
            "--kubeconfig",
            output,
        )
        logger.info("Kubeconfig exported to %s", output)
        return output

    async def list_clusters(self) -> list[str]:
        output = await self._run("get", "clusters")
        if not output:
            return []
        return [line.strip() for line in output.splitlines() if line.strip()]

    async def cluster_exists(self, name: str) -> bool:
        clusters = await self.list_clusters()
        return name in clusters

    # ---- Docker network operations ----

    async def get_own_network(self) -> str:
        """Discover the Docker network this container is connected to.

        Uses the container's hostname (which is the container ID in Docker)
        to inspect its network settings and return the first network name.
        """
        container_id = socket.gethostname()
        output = await self._docker(
            "inspect",
            "-f",
            "{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}",
            container_id,
        )
        networks = output.strip().split()
        if not networks:
            msg = "Could not determine Docker network for this container"
            raise RuntimeError(msg)
        # Return the first non-default network (prefer app-network over bridge)
        for net in networks:
            if net != "bridge":
                logger.info("Detected own Docker network: %s", net)
                return net
        logger.info("Detected own Docker network: %s", networks[0])
        return networks[0]

    async def connect_to_network(self, cluster_name: str, network: str) -> None:
        """Connect the Kind control-plane container to the given Docker network."""
        container_name = f"{cluster_name}-control-plane"
        logger.info(
            "Connecting %s to network %s",
            container_name,
            network,
        )
        await self._docker("network", "connect", network, container_name)
        logger.info("Connected %s to network %s", container_name, network)

    async def get_control_plane_ip(
        self,
        name: str,
        network: str | None = None,
    ) -> str:
        """Get the Docker network IP of the Kind control-plane container.

        If ``network`` is specified, returns the IP on that specific network.
        Otherwise returns the first IP found across all networks.
        """
        container_name = f"{name}-control-plane"

        if network:
            fmt = '{{(index .NetworkSettings.Networks "' + network + '").IPAddress}}'
        else:
            fmt = "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}"

        ip_address = await self._docker("inspect", "-f", fmt, container_name)

        if not ip_address:
            msg = f"Could not determine IP for container {container_name}"
            raise RuntimeError(msg)

        logger.info("Control-plane container %s has IP %s", container_name, ip_address)
        return ip_address

    # ---- Kubeconfig rewriting ----

    async def rewrite_kubeconfig_server(
        self,
        kubeconfig_path: str | Path,
        control_plane_ip: str,
        internal_port: int = 6443,
    ) -> None:
        """Rewrite kubeconfig server URL to use the container's Docker network IP.

        Also disables TLS verification because the Kind API server certificate
        is issued for 127.0.0.1/localhost, not the container network IP.
        """
        path = Path(kubeconfig_path)
        content = path.read_text(encoding="utf-8")
        kubeconfig = yaml.safe_load(content)

        new_server = f"https://{control_plane_ip}:{internal_port}"
        for cluster in kubeconfig.get("clusters", []):
            old_server = cluster.get("cluster", {}).get("server", "")
            cluster["cluster"]["server"] = new_server
            cluster["cluster"]["insecure-skip-tls-verify"] = True
            cluster["cluster"].pop("certificate-authority-data", None)
            logger.info("Rewrote kubeconfig server: %s -> %s", old_server, new_server)

        output = yaml.safe_dump(kubeconfig, sort_keys=False)
        await asyncio.to_thread(path.write_text, output, "utf-8")
