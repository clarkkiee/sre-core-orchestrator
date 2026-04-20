"""Exceptions for the provisioning agent integration."""


class AgentError(Exception):
    """Base exception for agent operations."""


class AgentUnreachableError(AgentError):
    """Raised when the agent cannot be reached via HTTP."""

    def __init__(self, host: str, port: int, detail: str = "") -> None:
        self.host = host
        self.port = port
        msg = f"Agent unreachable at {host}:{port}"
        if detail:
            msg += f": {detail}"
        super().__init__(msg)


class AgentTaskFailedError(AgentError):
    """Raised when an agent task finishes with status 'failed'."""

    def __init__(self, task_id: str, error: str) -> None:
        self.task_id = task_id
        self.agent_error = error
        super().__init__(f"Agent task {task_id} failed: {error}")


class AgentBootstrapError(AgentError):
    """Raised when agent installation or startup fails."""
