"""SSH-based agent installation and startup on the remote host."""

from __future__ import annotations

import asyncio
import logging
import secrets
from pathlib import Path

from app.infrastructure.agent.client import AgentClient
from app.infrastructure.agent.exceptions import AgentBootstrapError
from app.infrastructure.multipass.ssh_manager import SSHConfig, SSHManager

logger = logging.getLogger(__name__)

_AGENT_BINARY_NAME = "orchestrator-agent"


class AgentBootstrap:
    """Installs and starts the Go provisioning agent on a remote host via SSH."""

    def __init__(
        self,
        ssh_config: SSHConfig,
        agent_binary_path: str,
        agent_port: int,
        agent_token: str,
        agent_host: str = "",
    ) -> None:
        self._ssh_config = ssh_config
        self._agent_binary_path = agent_binary_path
        self._agent_port = agent_port
        self._agent_token = agent_token
        # If agent_host is not specified, use the SSH host.
        self._agent_host = agent_host or ssh_config.host
        # Track the actual token used (may differ from configured if generated).
        self._active_token = agent_token

    @property
    def agent_base_url(self) -> str:
        """Return the HTTP base URL for the agent."""
        return f"http://{self._agent_host}:{self._agent_port}"

    @property
    def active_token(self) -> str:
        """Return the token the running agent was started with."""
        return self._active_token

    async def ensure_running(self) -> None:
        """Check if agent is running; if not, install and start it."""
        client = AgentClient(
            base_url=self.agent_base_url,
            token=self._active_token,
        )
        try:
            if await client.health():
                logger.info("Agent already running at %s", self.agent_base_url)
                return
        finally:
            await client.close()

        logger.info("Agent not running; bootstrapping via SSH")
        self._active_token = await self._install_and_start()

        # Wait for agent to become healthy.
        await self._wait_healthy(timeout=30)

    async def _install_and_start(self) -> str:
        """SSH into the host, upload the binary, and start the agent.

        Returns the auth token the agent was started with.
        """
        ssh = SSHManager(self._ssh_config)
        async with ssh:
            # Expand ~ to absolute path for SFTP.
            _, home_dir, _ = await ssh.run("echo $HOME")
            home_dir = home_dir.strip()

            remote_binary = f"{home_dir}/.orchestrator-agent/bin/{_AGENT_BINARY_NAME}"
            remote_data_dir = f"{home_dir}/.orchestrator-agent"

            # Ensure directories exist.
            await ssh.run(
                f"mkdir -p {home_dir}/.orchestrator-agent/bin"
                f" {home_dir}/.orchestrator-agent/tasks"
                f" {home_dir}/.orchestrator-agent/cloud-init",
            )

            # Upload the agent binary via SFTP.
            local_binary = Path(self._agent_binary_path)
            if not local_binary.exists():
                msg = f"Agent binary not found at {self._agent_binary_path}"
                raise AgentBootstrapError(msg)

            logger.info("Uploading agent binary to %s", remote_binary)
            binary_content = local_binary.read_bytes()
            await self._sftp_upload(ssh, binary_content, remote_binary)

            # Make executable.
            await ssh.run(f"chmod +x {remote_binary}")

            # Kill any existing agent process (best-effort).
            # Bracket trick [o] prevents pkill from matching itself.
            await ssh.run(
                "pkill -f '[o]rchestrator-agent --' || true",
                check=False,
            )
            await asyncio.sleep(1)

            # Resolve token: use configured, or generate ephemeral.
            token = self._agent_token
            if not token:
                token = secrets.token_hex(32)
                logger.warning(
                    "No AGENT_API_TOKEN configured; generated ephemeral token: %s...",
                    token[:8],
                )

            start_cmd = (
                f"nohup {remote_binary}"
                f" --listen :{self._agent_port}"
                f" --token {token}"
                f" --data-dir {remote_data_dir}"
                f" > {remote_data_dir}/agent.log 2>&1 &"
            )
            await ssh.run(start_cmd, check=False)

            # Give the process a moment to start (or crash).
            await asyncio.sleep(2)

            # Verify the agent process is actually running.
            rc, _, _ = await ssh.run(
                "pgrep -f '[o]rchestrator-agent --'",
                check=False,
            )
            if rc != 0:
                _, log_tail, _ = await ssh.run(
                    f"tail -20 {remote_data_dir}/agent.log 2>/dev/null"
                    " || echo '(no log file found)'",
                    check=False,
                )
                msg = (
                    f"Agent process exited immediately after start.\n"
                    f"Agent log:\n{log_tail}"
                )
                raise AgentBootstrapError(msg)

            logger.info("Agent process confirmed running on remote host")

            # Quick connectivity check from the host itself.
            rc, curl_out, _ = await ssh.run(
                f"curl -s -o /dev/null -w '%{{http_code}}'"
                f" http://127.0.0.1:{self._agent_port}/health"
                " || echo 'curl_failed'",
                check=False,
            )
            if "curl_failed" in curl_out or curl_out.strip() != "200":
                _, log_tail, _ = await ssh.run(
                    f"tail -20 {remote_data_dir}/agent.log 2>/dev/null"
                    " || echo '(no log)'",
                    check=False,
                )
                logger.warning(
                    "Agent process is running but health check from host "
                    "returned: %s\nAgent log:\n%s",
                    curl_out, log_tail,
                )
            else:
                logger.info(
                    "Agent health OK from host (localhost:%d)",
                    self._agent_port,
                )

        return token

    async def _sftp_upload(
        self,
        ssh: SSHManager,
        content: bytes,
        remote_path: str,
    ) -> None:
        """Upload binary content to a remote path via SFTP."""
        conn = ssh._conn  # noqa: SLF001
        if conn is None:
            msg = "SSH connection not established"
            raise AgentBootstrapError(msg)

        async with conn.start_sftp_client() as sftp:
            async with sftp.open(remote_path, "wb") as f:
                await f.write(content)

    async def _wait_healthy(self, timeout: int = 30) -> None:
        """Poll agent health endpoint until it responds."""
        url = self.agent_base_url
        logger.info("Waiting for agent at %s ...", url)

        client = AgentClient(
            base_url=url,
            token=self._active_token,
        )
        try:
            elapsed = 0
            poll = 2
            while elapsed < timeout:
                if await client.health():
                    logger.info("Agent healthy at %s", url)
                    return
                await asyncio.sleep(poll)
                elapsed += poll

            msg = (
                f"Agent not healthy after {timeout}s at {url}. "
                f"If the orchestrator runs inside Docker, make sure "
                f"AGENT_HOST is set to the Docker bridge IP "
                f"(e.g. 172.17.0.1), not 127.0.0.1."
            )
            raise AgentBootstrapError(msg)
        finally:
            await client.close()
