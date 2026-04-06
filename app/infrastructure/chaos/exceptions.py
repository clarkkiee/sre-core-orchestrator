"""Chaos-specific exceptions"""


class LitmusCommandError(Exception):
    """Raised when a Litmus CLI command fails"""

    def __init__(self, command: str, returncode: int, stderr: str) -> None:
        self.command = command
        self.returncode = returncode
        self.stderr = stderr
        super().__init__(
            f"Litmus command '{command}' failed (rc={returncode}): {stderr}"
        )


class LitmusDeployError(Exception):
    """Raised when any Litmus Chaos components fails to deploy"""


class LitmusChaosExperimentError(Exception):
    """Raised when any Chaos Experiment Injection or Exception fails"""
