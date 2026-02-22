"""Celery application and task definitions."""

import os
from typing import Any

from celery import Celery

# Build broker URL from environment variables
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = os.getenv("RABBITMQ_PORT", "5672")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")

CELERY_BROKER_URL = os.getenv(
    "CELERY_BROKER_URL",
    f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}@{RABBITMQ_HOST}:{RABBITMQ_PORT}//",
)
CELERY_RESULT_BACKEND = os.getenv(
    "CELERY_RESULT_BACKEND",
    f"rpc://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}@{RABBITMQ_HOST}:{RABBITMQ_PORT}//",
)

# Initialize Celery app
celery_app = Celery(
    "chaos_platform",
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND,
)

# Celery configuration for long-running tasks
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    # Long-running task settings
    task_acks_late=True,  # Acknowledge after task completion (crash recovery)
    task_reject_on_worker_lost=True,  # Requeue if worker dies
    worker_prefetch_multiplier=1,  # Fair task distribution
    task_time_limit=3600,  # 1 hour hard limit
    task_soft_time_limit=3300,  # 55 min soft limit (allows cleanup)
    # Result settings
    result_expires=86400,  # Results expire after 24 hours
    # Retry settings
    task_default_retry_delay=60,  # 1 minute between retries
    task_max_retries=3,
)

# Auto-discover tasks from app modules
celery_app.autodiscover_tasks(["app"])


@celery_app.task(bind=True, name="app.tasks.example_task")  # type: ignore[misc]
def example_task(self: Any, param: str) -> dict[str, str]:  # noqa: ANN401
    """Example placeholder task.

    Args:
        self: Celery task instance (injected by bind=True)
        param: Example parameter

    Returns:
        Task result dictionary
    """
    _ = self  # Available for retries: self.retry()
    return {"status": "completed", "param": param}


@celery_app.task(bind=True, name="app.tasks.long_running_chaos_experiment")  # type: ignore[misc]
def long_running_chaos_experiment(
    self: Any,  # noqa: ANN401
    experiment_id: str,
    config: dict[str, Any],
) -> dict[str, Any]:
    """Placeholder for long-running chaos experiment task.

    This task is designed for:
    - Crash recovery (task_acks_late=True)
    - Long execution times (up to 1 hour)
    - Automatic retry on failure

    Args:
        self: Celery task instance (injected by bind=True)
        experiment_id: Unique experiment identifier
        config: Experiment configuration

    Returns:
        Experiment result dictionary
    """
    _ = self  # Available for retries: self.retry()
    # TODO: Implement chaos experiment logic
    return {
        "experiment_id": experiment_id,
        "status": "completed",
        "config": config,
    }
