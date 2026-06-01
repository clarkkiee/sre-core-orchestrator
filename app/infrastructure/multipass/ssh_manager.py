"""Async SSH connection manager for remote command execution.

Uses ``asyncssh`` to run commands on a remote (or local) host where
Multipass is installed.  This lets the orchestrator run inside Docker
while Multipass runs on the host machine.

Typical localhost usage::

    manager = SSHManager(SSHConfig(host="127.0.0.1", username="myuser"))
    await manager.connect()
    rc, stdout, stderr = await manager.run("multipass list")
    await manager.disconnect()
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass

import asyncssh

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SSHConfig:
    """SSH connection parameters."""

    host: str = "127.0.0.1"
    port: int = 22
    username: str = ""
    private_key_path: str | None = None
    password: str | None = None
    known_hosts: str | None = None  # None → accept any host key
    connect_timeout: int = 30


class SSHConnectionError(Exception):
    """Raised when the SSH connection cannot be established."""


class SSHCommandError(Exception):
    """Raised when a remote command exits with a non-zero status."""

    def __init__(
        self, command: str, returncode: int, stderr: str,
    ) -> None:
        self.command = command
        self.returncode = returncode
        self.stderr = stderr
        super().__init__(
            f"SSH command failed (rc={returncode}): {command}\n{stderr}",
        )


class SSHManager:
    """Manages an async SSH connection for remote command execution.

    The connection is lazy — call :meth:`connect` explicitly or let
    :meth:`ensure_connected` handle it on first use.
    """

    def __init__(self, config: SSHConfig) -> None:
        self._config = config
        self._conn: asyncssh.SSHClientConnection | None = None

    # ---- connection lifecycle ----

    async def connect(self) -> None:
        """Establish (or re-establish) the SSH connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

        kwargs: dict = {
            "host": self._config.host,
            "port": self._config.port,
            "login_timeout": self._config.connect_timeout,
        }

        if self._config.username:
            kwargs["username"] = self._config.username
        if self._config.private_key_path:
            kwargs["client_keys"] = [self._config.private_key_path]
        if self._config.password:
            kwargs["password"] = self._config.password

        # known_hosts=None → don't verify (common for localhost / lab envs)
        if self._config.known_hosts is None:
            kwargs["known_hosts"] = None
        else:
            kwargs["known_hosts"] = self._config.known_hosts

        try:
            self._conn = await asyncssh.connect(**kwargs)
            logger.info(
                "SSH connected to %s@%s:%d",
                self._config.username or "(default)",
                self._config.host,
                self._config.port,
            )
        except (OSError, asyncssh.Error) as exc:
            msg = (
                f"Cannot connect to {self._config.host}:{self._config.port} "
                f"as {self._config.username or '(default)'}: {exc}"
            )
            raise SSHConnectionError(msg) from exc

    async def disconnect(self) -> None:
        """Close the SSH connection if open."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            logger.info("SSH disconnected from %s", self._config.host)

    async def ensure_connected(self) -> None:
        """Connect if not already connected, or if the connection dropped."""
        if self._conn is None or self._conn.is_closed():
            await self.connect()

    # ---- command execution ----

    async def run(
        self,
        command: str,
        *,
        check: bool = True,
    ) -> tuple[int, str, str]:
        """Execute a shell command on the remote host.

        Args:
            command: The shell command string to execute.
            check:  If True, raise :class:`SSHCommandError` on non-zero exit.

        Returns:
            ``(returncode, stdout, stderr)``
        """
        await self.ensure_connected()
        assert self._conn is not None  # noqa: S101

        logger.debug("SSH exec: %s", command)

        result = await self._conn.run(command, check=False)

        rc = result.returncode or 0
        stdout = (result.stdout or "").strip()
        stderr = (result.stderr or "").strip()

        if check and rc != 0:
            raise SSHCommandError(command=command, returncode=rc, stderr=stderr)

        if stderr:
            logger.debug("SSH stderr: %s", stderr)

        return rc, stdout, stderr

    # ---- file operations ----

    async def write_remote_tempfile(
        self,
        content: str,
        *,
        suffix: str = ".tmp",
        prefix: str = "orch-",
    ) -> str:
        """Write *content* to a unique temp file on the remote host.

        Uses SFTP for reliable binary-safe file transfer.  Files are
        placed under ``$HOME/.orchestrator-tmp/`` so that snap-confined
        applications (like multipass) can read them.

        Returns the remote file path.  The caller is responsible for
        calling :meth:`remove_remote_file` when done.
        """
        unique = uuid.uuid4().hex[:12]

        # Discover the remote user's home directory
        _, home_dir, _ = await self.run("echo $HOME", check=True)
        staging_dir = f"{home_dir}/.orchestrator-tmp"
        await self.run(f"mkdir -p '{staging_dir}'", check=True)

        remote_path = f"{staging_dir}/{prefix}{unique}{suffix}"

        # Use SFTP for reliable file transfer (no shell escaping issues)
        await self.ensure_connected()
        assert self._conn is not None  # noqa: S101

        async with (
            self._conn.start_sftp_client() as sftp,
            sftp.open(remote_path, "w") as f,
        ):
            await f.write(content)

        # Ensure readable by snap daemons (multipassd runs as root)
        await self.run(f"chmod 644 '{remote_path}'", check=True)

        logger.info("Wrote remote temp file: %s (%d bytes)", remote_path, len(content))
        return remote_path

    async def remove_remote_file(self, remote_path: str) -> None:
        """Remove a file on the remote host (best-effort)."""
        try:
            await self.run(f"rm -f '{remote_path}'", check=False)
            logger.debug("Removed remote file: %s", remote_path)
        except SSHConnectionError:
            logger.warning("Could not remove remote file %s (SSH down)", remote_path)

    # ---- context manager ----

    async def __aenter__(self) -> SSHManager:
        await self.connect()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.disconnect()
