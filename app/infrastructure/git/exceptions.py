"""Git-specific exceptions."""


class GitCloneError(Exception):
    """Raised when a git clone operation fails."""

    def __init__(self, repo_url: str, branch: str, detail: str) -> None:
        self.repo_url = repo_url
        self.branch = branch
        self.detail = detail
        super().__init__(f"Failed to clone '{repo_url}' (branch: {branch}): {detail}")
