"""Infrastructure-specific exceptions."""


class KindCommandError(Exception):
    """Raised when a Kind CLI command fails."""

    def __init__(self, command: str, returncode: int, stderr: str) -> None:
        self.command = command
        self.returncode = returncode
        self.stderr = stderr
        super().__init__(f"Kind command '{command}' failed (rc={returncode}): {stderr}")


class PortExhaustionError(Exception):
    """Raised when no port blocks are available for allocation."""

    def __init__(self, message: str = "No available port blocks") -> None:
        super().__init__(message)


class DeployerCommandError(Exception):
    """Raised when a deployer CLI command fails."""

    def __init__(self, command: str, returncode: int, stderr: str) -> None:
        self.command = command
        self.returncode = returncode
        self.stderr = stderr
        super().__init__(
            f"Deployer command '{command}' failed (rc={returncode}): {stderr}"
        )


class GitCloneError(Exception):
    """Raised when a git clone operation fails."""

    def __init__(self, repo_url: str, branch: str, detail: str) -> None:
        self.repo_url = repo_url
        self.branch = branch
        self.detail = detail
        super().__init__(f"Failed to clone '{repo_url}' (branch: {branch}): {detail}")
