"""Placeholder tasks for future implementation."""

from typing import Any

from app.tasks.celery_config import celery_app


@celery_app.task(bind=True, name="app.tasks.example_task")  # type: ignore[misc]
def example_task(self: Any, param: str) -> dict[str, str]:  # noqa: ANN401
    """Example placeholder task."""
    _ = self  # Available for retries: self.retry()
    return {"status": "completed", "param": param}
