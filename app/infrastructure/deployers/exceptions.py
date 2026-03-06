"""Deployer-specific exceptions."""


class DeployerCommandError(Exception):
    """Raised when a deployer CLI command fails."""

    def __init__(self, command: str, returncode: int, stderr: str) -> None:
        self.command = command
        self.returncode = returncode
        self.stderr = stderr
        super().__init__(
            f"Deployer command '{command}' failed (rc={returncode}): {stderr}"
        )
