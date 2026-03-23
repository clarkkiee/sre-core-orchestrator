"""linkerd-specific exceptions."""


class LinkerdCommandError(Exception):
    """Raised when a linkerd CLI command fails"""

    def __init__(self, command: str, returncode: int, stderr: str) -> None:
        self.command = command
        self.returncode = returncode
        self.stderr = stderr
        super().__init__(
            f"Linkerd command '{command}' failed (rc={returncode}): {stderr}"
        )


class LinkerdDeployError(Exception):
    """Raised when the Linkerd service mesh fails to deploy."""
