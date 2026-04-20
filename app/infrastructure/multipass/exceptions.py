"""Multipass-specific exceptions"""

class MultipassError(Exception):
    """Base exception for Multipass operations"""
    pass

class MultipassCommandError(MultipassError):
    """Raised when a Multipass command fails"""

    def __init__(self, command: str, returncode: int, stderr: str) -> None:
        self.command = command
        self.returncode = returncode
        self.stderr = stderr
        msg = (
            f"Multipass command '{command}' failed\n"
            f"(rc={returncode}): {stderr}"
        )
        super().__init__(msg)

class MultipassVMNotFoundError(MultipassError):
    """Raised when no VM are available for allocation."""

    def __init__(self, vm_name: str) -> None:
        self.vm_name = vm_name
        msg = f"VM {vm_name!r} not found"
        super().__init__(msg)

class K3sBootstrapError(MultipassError):
    """Raised when k3s bootstrap operations fail"""

class CloudInitError(MultipassError):
    """Raised when cloud-init generations fail"""


class SSHConnectionError(MultipassError):
    """Raised when the SSH connection to the Multipass host fails."""


class SSHCommandError(MultipassError):
    """Raised when a command executed over SSH exits with non-zero status."""

    def __init__(self, command: str, returncode: int, stderr: str) -> None:
        self.command = command
        self.returncode = returncode
        self.stderr = stderr
        msg = (
            f"SSH command failed (rc={returncode}): {command}\n{stderr}"
        )
        super().__init__(msg)
