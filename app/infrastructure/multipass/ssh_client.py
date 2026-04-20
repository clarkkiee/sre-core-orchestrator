"""Multipass client that executes commands over SSH.

Drop-in replacement for :class:`MultipassClient` (``client.py``) — same
public interface, but every command runs on a remote host via
:class:`SSHManager` instead of a local subprocess.

This lets the orchestrator run inside Docker while Multipass stays on
the host (or any reachable machine with ``sshd`` + ``multipass``).
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from app.infrastructure.multipass.exceptions import (
    MultipassCommandError,
    MultipassVMNotFoundError,
)

if TYPE_CHECKING:
    from app.infrastructure.multipass.ssh_manager import SSHManager

logger = logging.getLogger(__name__)


class MultipassSSHClient:
    """Multipass CLI wrapper that executes commands over SSH.

    Mirrors the :class:`MultipassClient` interface so that
    ``K3sBootstrapper`` and the provider work without changes.
    """

    def __init__(
        self,
        ssh_manager: SSHManager,
        multipass_binary: str = "/snap/bin/multipass",
    ) -> None:
        self._ssh = ssh_manager
        self._binary = multipass_binary

    # ---- internal helpers ----

    async def _run(
        self,
        *args: str,
        check: bool = True,
    ) -> str:
        """Execute a multipass command on the remote host, return stdout."""
        # Shell-quote each argument to handle spaces / special chars
        quoted = " ".join(_shell_quote(a) for a in args)
        cmd = f"{self._binary} {quoted}"
        logger.info("SSH multipass: %s", cmd)

        try:
            rc, stdout, stderr = await self._ssh.run(cmd, check=False)
        except Exception as exc:
            if check:
                raise MultipassCommandError(
                    command=cmd, returncode=1, stderr=str(exc),
                ) from exc
            return ""

        if check and rc != 0:
            raise MultipassCommandError(
                command=cmd, returncode=rc, stderr=stderr,
            )

        if stderr:
            logger.debug("multipass stderr: %s", stderr)

        return stdout

    async def _run_rc(self, *args: str) -> tuple[int, str, str]:
        """Execute a multipass command, return (rc, stdout, stderr)."""
        quoted = " ".join(_shell_quote(a) for a in args)
        cmd = f"{self._binary} {quoted}"
        logger.debug("SSH multipass: %s", cmd)

        return await self._ssh.run(cmd, check=False)

    # ---- VM lifecycle ----

    async def launch_vm(  # noqa: PLR0913
        self,
        vm_name: str,
        *,
        cpus: int = 2,
        memory: str = "2G",
        disk: str = "10G",
        cloud_init_file: str | None = None,
        image: str = "22.04",
    ) -> None:
        """Launch a new Multipass VM.

        ``cloud_init_file`` must be a path **on the remote host** (use
        :meth:`SSHManager.write_remote_tempfile` to create one).
        """
        args = [
            "launch",
            "--name", vm_name,
            "--cpus", str(cpus),
            "--memory", memory,
            "--disk", disk,
        ]
        if cloud_init_file:
            args.extend(["--cloud-init", cloud_init_file])
        args.append(image)

        await self._run(*args)
        logger.info("VM %s launched successfully (via SSH)", vm_name)

    async def delete_vm(self, vm_name: str) -> None:
        """Stop (if running) then delete and purge a VM."""
        await self._run_rc("stop", vm_name)
        await self._run("delete", vm_name, "--purge")
        logger.info("VM %s deleted and purged (via SSH)", vm_name)

    async def vm_exists(self, vm_name: str) -> bool:
        """Return True if a VM with this name exists."""
        rc, _, _ = await self._run_rc("info", vm_name)
        return rc == 0

    async def get_vm_ip(self, vm_name: str) -> str:
        """Get the primary IPv4 address of a running VM."""
        if not await self.vm_exists(vm_name):
            raise MultipassVMNotFoundError(vm_name)

        stdout = await self._run("info", vm_name, "--format", "json")
        try:
            info = json.loads(stdout)
            ipv4_list = info["info"][vm_name]["ipv4"]
            if not ipv4_list:
                msg = f"VM {vm_name!r} has no IPv4 addresses"
                raise ValueError(msg)
            return ipv4_list[0]
        except (json.JSONDecodeError, KeyError, IndexError) as exc:
            msg = f"Failed to parse Multipass info for {vm_name!r}: {exc}"
            raise ValueError(msg) from exc

    async def list_vms(self) -> list[dict[str, str]]:
        """List all VMs with name, state, and IP."""
        stdout = await self._run("list", "--format", "json")
        try:
            data = json.loads(stdout)
            return [
                {
                    "name": vm["name"],
                    "state": vm["state"],
                    "ipv4": vm.get("ipv4", [""])[0],
                }
                for vm in data.get("list", [])
            ]
        except (json.JSONDecodeError, KeyError) as exc:
            msg = f"Failed to parse multipass list output: {exc}"
            raise ValueError(msg) from exc

    # ---- Remote execution (multipass exec) ----

    async def exec_in_vm(
        self,
        vm_name: str,
        *cmd: str,
        check: bool = True,
    ) -> str:
        """Execute a command inside a VM via ``multipass exec``."""
        return await self._run("exec", vm_name, "--", *cmd, check=check)

    async def exec_in_vm_rc(
        self,
        vm_name: str,
        *cmd: str,
    ) -> tuple[int, str, str]:
        """Execute a command inside a VM, returning (rc, stdout, stderr)."""
        return await self._run_rc("exec", vm_name, "--", *cmd)

    # ---- File transfer ----

    async def transfer_file(
        self,
        local_path: str,
        vm_name: str,
        remote_path: str,
    ) -> None:
        """Transfer a file to a VM.

        Note: ``local_path`` here refers to a path on the **SSH host**,
        not the orchestrator container.  Use :meth:`SSHManager.write_remote_tempfile`
        to stage files from the orchestrator first if needed.
        """
        await self._run("transfer", local_path, f"{vm_name}:{remote_path}")
        logger.info("Transferred %s -> %s:%s (via SSH)", local_path, vm_name, remote_path)

    # ---- Readiness ----

    async def wait_for_cloud_init(
        self,
        vm_name: str,
        timeout_seconds: int = 300,
    ) -> None:
        """Wait until cloud-init finishes inside the VM."""
        import asyncio

        elapsed = 0
        poll_interval = 5

        while elapsed < timeout_seconds:
            rc, stdout, _ = await self.exec_in_vm_rc(
                vm_name, "cloud-init", "status",
            )
            if rc == 0 and "done" in stdout.lower():
                logger.info("cloud-init finished on VM %s", vm_name)
                return

            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

        msg = f"cloud-init on VM {vm_name!r} not done after {timeout_seconds}s"
        raise TimeoutError(msg)


def _shell_quote(s: str) -> str:
    """Safely quote a string for POSIX shell embedding."""
    if not s:
        return "''"
    # If the string only contains safe characters, return as-is
    safe_chars = frozenset(
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "0123456789"
        "/_.-=:,+@"
    )
    if all(c in safe_chars for c in s):
        return s
    # Otherwise wrap in single quotes, escaping any existing single quotes
    return "'" + s.replace("'", "'\\''") + "'"
