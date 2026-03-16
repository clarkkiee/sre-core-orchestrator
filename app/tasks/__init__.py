"""Celery tasks package.

Re-exports celery_app and all task functions for backward compatibility.
Importing task modules here ensures Celery discovers them.
"""

from app.tasks.celery_config import celery_app
from app.tasks.cluster_tasks import (
    provision_cluster_task,
    reconnect_cluster_task,
    teardown_cluster_task,
)
from app.tasks.deployment_tasks import delete_deployment_task, deploy_application_task
from app.tasks.observability_tasks import collect_metrics_task
from app.tasks.placeholder_tasks import (
    example_task,
    long_running_chaos_experiment,
)

__all__ = [
    "celery_app",
    "collect_metrics_task",
    "delete_deployment_task",
    "deploy_application_task",
    "example_task",
    "long_running_chaos_experiment",
    "provision_cluster_task",
    "reconnect_cluster_task",
    "teardown_cluster_task",
]
