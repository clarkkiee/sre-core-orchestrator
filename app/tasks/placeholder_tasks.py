"""Placeholder tasks for future implementation."""

from typing import Any

from app.tasks.celery_config import celery_app


@celery_app.task(bind=True, name="app.tasks.example_task")  # type: ignore[misc]
def example_task(self: Any, param: str) -> dict[str, str]:  # noqa: ANN401
    """Example placeholder task."""
    _ = self  # Available for retries: self.retry()
    return {"status": "completed", "param": param}


@celery_app.task(bind=True, name="app.tasks.long_running_chaos_experiment")  # type: ignore[misc]
def long_running_chaos_experiment(
    self: Any,  # noqa: ANN401
    experiment_id: str,
    config: dict[str, Any],
) -> dict[str, Any]:
    """Placeholder for long-running chaos experiment task."""
    _ = self  # Available for retries: self.retry()
    # TODO: Implement chaos experiment logic
    return {
        "experiment_id": experiment_id,
        "status": "completed",
        "config": config,
    }
