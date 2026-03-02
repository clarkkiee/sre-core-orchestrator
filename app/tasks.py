"""Celery application and task definitions."""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from celery import Celery
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.infrastructure.kind import KindClient
from app.infrastructure.kind_config import KindConfigBuilder
from app.infrastructure.kubernetes_verifier import KubernetesVerifier
from app.models.cluster import ClusterStatus
from app.models.job import JobStatus
from app.repositories.cluster import ClusterRepository
from app.repositories.job import JobRepository
from app.utils.config import settings

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

logger = logging.getLogger(__name__)


def _make_session_maker() -> async_sessionmaker[AsyncSession]:
    """Create a fresh async engine + session maker for each task invocation.

    This avoids the 'Future attached to a different loop' error that occurs
    when a module-level engine is reused across multiple ``asyncio.run()``
    calls in Celery's forked worker processes.
    """
    task_engine = create_async_engine(
        settings.DATABASE_URL,
        pool_size=settings.POSTGRES_POOL_SIZE,
        max_overflow=settings.POSTGRES_MAX_OVERFLOW,
        pool_timeout=settings.POSTGRES_POOL_TIMEOUT,
        echo=settings.LOG_LEVEL == "DEBUG",
        future=True,
    )
    return async_sessionmaker(
        task_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
        autocommit=False,
    )


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


# ---------------------------------------------------------------------------
# Cluster provisioning / teardown tasks
# ---------------------------------------------------------------------------


@celery_app.task(  # type: ignore[misc]
    bind=True,
    name="app.tasks.provision_cluster",
    max_retries=1,
    soft_time_limit=1800,
    time_limit=1860,
)
def provision_cluster_task(
    self: Any,  # noqa: ANN401
    cluster_id: str,
    job_id: str,
) -> dict[str, str]:
    """Provision a KinD cluster (dispatched by ClusterService)."""
    _ = self
    return asyncio.run(_provision_cluster(cluster_id, job_id))


@celery_app.task(  # type: ignore[misc]
    bind=True,
    name="app.tasks.teardown_cluster",
    max_retries=1,
    soft_time_limit=300,
    time_limit=360,
)
def teardown_cluster_task(
    self: Any,  # noqa: ANN401
    cluster_id: str,
    job_id: str,
) -> dict[str, str]:
    """Teardown a KinD cluster (dispatched by ClusterService)."""
    _ = self
    return asyncio.run(_teardown_cluster(cluster_id, job_id))


@celery_app.task(  # type: ignore[misc]
    bind=True,
    name="app.tasks.reconnect_cluster",
    max_retries=1,
    soft_time_limit=300,
    time_limit=360,
)
def reconnect_cluster_task(
    self: Any,  # noqa: ANN401
    cluster_id: str,
    job_id: str,
) -> dict[str, str]:
    """Reconnect a KinD cluster (dispatched by ClusterService)."""
    _ = self
    return asyncio.run(_reconnect_cluster(cluster_id, job_id))


# ---------------------------------------------------------------------------
# Async provisioning implementation
# ---------------------------------------------------------------------------


async def _record_failure(
    cluster_id: uuid.UUID,
    job_id: uuid.UUID,
    exc: Exception,
    *,
    prefix: str = "",
) -> None:
    """Record a failure on both Cluster and Job in a fresh DB session."""
    async with _make_session_maker()() as err_session:
        c_repo = ClusterRepository(err_session)
        j_repo = JobRepository(err_session)
        cluster = await c_repo.get_by_id(cluster_id)
        job = await j_repo.get_by_id(job_id)
        status_msg = f"{prefix}{exc!s}" if prefix else str(exc)
        if cluster:
            await c_repo.update(
                cluster,
                status=ClusterStatus.FAILED,
                status_message=status_msg[:500],
            )
        if job:
            await j_repo.update(
                job,
                status=JobStatus.FAILED,
                error_message=str(exc)[:1000],
                completed_at=datetime.now(UTC),
            )
        await err_session.commit()


async def _run_provisioning_phases(
    cluster_repo: ClusterRepository,
    job_repo: JobRepository,
    cluster: Any,  # noqa: ANN401
    job: Any,  # noqa: ANN401
    session: Any,  # noqa: ANN401
) -> dict[str, str]:
    """Execute the provisioning phases in order."""
    kind_client = KindClient(kind_binary=settings.KIND_BINARY)
    config_builder = KindConfigBuilder(
        port_range_start=settings.KIND_PORT_RANGE_START,
        port_range_end=settings.KIND_PORT_RANGE_END,
        ports_per_block=settings.KIND_PORTS_PER_BLOCK,
    )
    verifier = KubernetesVerifier()

    # Phase 1: BUILDING_CONFIG
    await cluster_repo.update(cluster, status=ClusterStatus.PROVISIONING)
    await job_repo.update(
        job,
        status=JobStatus.RUNNING,
        started_at=datetime.now(UTC),
        current_phase="BUILDING_CONFIG",
        progress_percentage=10,
    )
    await session.commit()

    ports_data = cluster.ports or {}
    worker_count = ports_data.get("worker_count", 2)
    config = config_builder.build_config(
        cluster_name=cluster.kind_name,
        ports_data=ports_data,
        worker_count=worker_count,
        registry_url=settings.PRIVATE_REGISTRY_URL,
    )
    config_path = config_builder.write_config(
        config,
        Path(settings.KUBECONFIG_DIR) / f"kind-config-{cluster.kind_name}.yaml",
    )

    # Phase 2: CREATING_CLUSTER
    await job_repo.update(
        job,
        current_phase="CREATING_CLUSTER",
        progress_percentage=30,
    )
    await session.commit()

    if not await kind_client.cluster_exists(cluster.kind_name):
        await kind_client.create_cluster(cluster.kind_name, str(config_path))

    # Phase 3: EXPORTING_KUBECONFIG
    await job_repo.update(
        job,
        current_phase="EXPORTING_KUBECONFIG",
        progress_percentage=60,
    )
    await session.commit()

    kubeconfig_path = str(
        Path(settings.KUBECONFIG_DIR) / f"kubeconfig-{cluster.kind_name}.yaml",
    )
    await kind_client.export_kubeconfig(cluster.kind_name, kubeconfig_path)

    # Connect the Kind control-plane to this container's Docker network
    # so the Celery worker can reach the API server directly.
    network = await kind_client.get_own_network()
    await kind_client.connect_to_network(cluster.kind_name, network)
    cp_ip = await kind_client.get_control_plane_ip(cluster.kind_name, network=network)
    kubeconfig_content = await kind_client.rewrite_kubeconfig_server(
        kubeconfig_path,
        cp_ip,
    )

    # Phase 4: VERIFYING
    await job_repo.update(
        job,
        current_phase="VERIFYING",
        progress_percentage=80,
    )
    await session.commit()

    await verifier.verify_cluster_ready(kubeconfig_content)

    # Phase 5: COMPLETE
    await cluster_repo.update(
        cluster,
        status=ClusterStatus.READY,
        kubeconfig=kubeconfig_content,
        status_message="Cluster provisioned successfully",
    )
    await job_repo.update(
        job,
        status=JobStatus.COMPLETED,
        current_phase="COMPLETE",
        progress_percentage=100,
        completed_at=datetime.now(UTC),
    )
    await session.commit()

    return {"status": "completed", "cluster_id": str(cluster.id)}


async def _provision_cluster(
    cluster_id: str,
    job_id: str,
) -> dict[str, str]:
    cid = uuid.UUID(cluster_id)
    jid = uuid.UUID(job_id)

    async with _make_session_maker()() as session:
        cluster_repo = ClusterRepository(session)
        job_repo = JobRepository(session)

        cluster = await cluster_repo.get_by_id(cid)
        job = await job_repo.get_by_id(jid)
        if not cluster or not job:
            msg = f"Cluster {cluster_id} or Job {job_id} not found"
            raise ValueError(msg)

        try:
            result = await _run_provisioning_phases(
                cluster_repo,
                job_repo,
                cluster,
                job,
                session,
            )
        except Exception as exc:
            await session.rollback()
            logger.exception(
                "Cluster provisioning failed",
                extra={"cluster_id": cluster_id, "job_id": job_id},
            )
            await _record_failure(cid, jid, exc)
            raise

    logger.info("Cluster %s provisioned successfully", cluster_id)
    return result


async def _teardown_cluster(
    cluster_id: str,
    job_id: str,
) -> dict[str, str]:
    kind_client = KindClient(kind_binary=settings.KIND_BINARY)
    cid = uuid.UUID(cluster_id)
    jid = uuid.UUID(job_id)

    async with _make_session_maker()() as session:
        cluster_repo = ClusterRepository(session)
        job_repo = JobRepository(session)

        cluster = await cluster_repo.get_by_id(cid)
        job = await job_repo.get_by_id(jid)
        if not cluster or not job:
            msg = f"Cluster {cluster_id} or Job {job_id} not found"
            raise ValueError(msg)

        try:
            await job_repo.update(
                job,
                status=JobStatus.RUNNING,
                started_at=datetime.now(UTC),
                current_phase="DELETING_CLUSTER",
                progress_percentage=20,
            )
            await session.commit()

            if await kind_client.cluster_exists(cluster.kind_name):
                await kind_client.delete_cluster(cluster.kind_name)

            await cluster_repo.update(
                cluster,
                status=ClusterStatus.DELETED,
                deleted_at=datetime.now(UTC),
                status_message="Cluster deleted successfully",
            )
            await job_repo.update(
                job,
                status=JobStatus.COMPLETED,
                current_phase="COMPLETE",
                progress_percentage=100,
                completed_at=datetime.now(UTC),
            )
            await session.commit()
        except Exception as exc:
            await session.rollback()
            logger.exception(
                "Cluster teardown failed",
                extra={"cluster_id": cluster_id, "job_id": job_id},
            )
            await _record_failure(cid, jid, exc, prefix="Teardown failed: ")
            raise

    logger.info("Cluster %s deleted successfully", cluster_id)
    return {"status": "deleted", "cluster_id": cluster_id}


async def _reconnect_cluster(
    cluster_id: str,
    job_id: str,
) -> dict[str, str]:
    """Re-discover control-plane IP, rewrite kubeconfig, and verify health."""
    from app.infrastructure.exceptions import KindCommandError

    kind_client = KindClient(kind_binary=settings.KIND_BINARY)
    verifier = KubernetesVerifier()
    cid = uuid.UUID(cluster_id)
    jid = uuid.UUID(job_id)

    async with _make_session_maker()() as session:
        cluster_repo = ClusterRepository(session)
        job_repo = JobRepository(session)

        cluster = await cluster_repo.get_by_id(cid)
        job = await job_repo.get_by_id(jid)
        if not cluster or not job:
            msg = f"Cluster {cluster_id} or Job {job_id} not found"
            raise ValueError(msg)

        try:
            # Phase 1: CHECK_CONTAINER (10%)
            await job_repo.update(
                job,
                status=JobStatus.RUNNING,
                started_at=datetime.now(UTC),
                current_phase="CHECK_CONTAINER",
                progress_percentage=10,
            )
            await session.commit()

            container_name = f"{cluster.kind_name}-control-plane"
            try:
                await kind_client._docker("inspect", container_name)  # noqa: SLF001
            except KindCommandError:
                await cluster_repo.update(
                    cluster,
                    status=ClusterStatus.FAILED,
                    status_message=(
                        "Kind container no longer exists. "
                        "Please delete this cluster and re-provision."
                    ),
                )
                await job_repo.update(
                    job,
                    status=JobStatus.FAILED,
                    current_phase="CHECK_CONTAINER",
                    error_message="Kind container not found."
                    "Cluster must be re-provisioned.",
                    completed_at=datetime.now(UTC),
                )
                await session.commit()
                return {
                    "status": "failed",
                    "cluster_id": cluster_id,
                    "reason": "container_not_found",
                }

            # Phase 2: RECONNECT_NETWORK (30%)
            await job_repo.update(
                job,
                current_phase="RECONNECT_NETWORK",
                progress_percentage=30,
            )
            await session.commit()

            network = await kind_client.get_own_network()
            try:
                await kind_client.connect_to_network(cluster.kind_name, network)
            except KindCommandError as exc:
                if "already exists" not in exc.stderr.lower():
                    raise

            # Phase 3: EXPORT_KUBECONFIG (50%)
            await job_repo.update(
                job,
                current_phase="EXPORT_KUBECONFIG",
                progress_percentage=50,
            )
            await session.commit()

            kubeconfig_path = str(
                Path(settings.KUBECONFIG_DIR) / f"kubeconfig-{cluster.kind_name}.yaml",
            )
            await kind_client.export_kubeconfig(cluster.kind_name, kubeconfig_path)

            cp_ip = await kind_client.get_control_plane_ip(
                cluster.kind_name,
                network=network,
            )
            kubeconfig_content = await kind_client.rewrite_kubeconfig_server(
                kubeconfig_path,
                cp_ip,
            )

            # Phase 4: VERIFYING (75%)
            await job_repo.update(
                job,
                current_phase="VERIFYING",
                progress_percentage=75,
            )
            await session.commit()

            await verifier.verify_cluster_ready(
                kubeconfig_content,
                timeout_seconds=60,
            )

            # Phase 5: COMPLETE (100%)
            await cluster_repo.update(
                cluster,
                status=ClusterStatus.READY,
                kubeconfig=kubeconfig_content,
                status_message="Cluster reconnected successfully",
            )
            await job_repo.update(
                job,
                status=JobStatus.COMPLETED,
                current_phase="COMPLETE",
                progress_percentage=100,
                completed_at=datetime.now(UTC),
            )
            await session.commit()

        except Exception as exc:
            await session.rollback()
            logger.exception(
                "Cluster reconnect failed",
                extra={"cluster_id": cluster_id, "job_id": job_id},
            )
            await _record_failure(cid, jid, exc, prefix="Reconnect failed: ")
            raise

    logger.info("Cluster %s reconnected successfully", cluster_id)
    return {"status": "reconnected", "cluster_id": cluster_id}


# ---------------------------------------------------------------------------
# Application deployment task
# ---------------------------------------------------------------------------


@celery_app.task(  # type: ignore[misc]
    bind=True,
    name="app.tasks.deploy_application",
    max_retries=1,
    soft_time_limit=900,
    time_limit=960,
)
def deploy_application_task(
    self: Any,  # noqa: ANN401
    deployment_id: str,
    job_id: str,
    github_token: str | None = None,
) -> dict[str, str]:
    """Deploy an application into a cluster (dispatched by DeploymentService)."""
    _ = self
    return asyncio.run(_deploy_application(deployment_id, job_id, github_token))


async def _record_deployment_failure(
    deployment_id: uuid.UUID,
    job_id: uuid.UUID,
    exc: Exception,
    *,
    prefix: str = "",
) -> None:
    """Record a failure on Deployment and Job in a fresh DB session."""
    from app.models.deployment import DeploymentStatus
    from app.repositories.deployment import DeploymentRepository

    async with _make_session_maker()() as err_session:
        d_repo = DeploymentRepository(err_session)
        j_repo = JobRepository(err_session)
        deployment = await d_repo.get_by_id(deployment_id)
        job = await j_repo.get_by_id(job_id)
        status_msg = f"{prefix}{exc!s}" if prefix else str(exc)
        if deployment:
            await d_repo.update(
                deployment,
                status=DeploymentStatus.FAILED,
                status_message=status_msg[:500],
            )
        if job:
            await j_repo.update(
                job,
                status=JobStatus.FAILED,
                error_message=str(exc)[:1000],
                completed_at=datetime.now(UTC),
            )
        await err_session.commit()


async def _run_deployment_phases(  # noqa: PLR0913, PLR0915
    deployment_repo: Any,  # noqa: ANN401
    job_repo: JobRepository,
    deployment: Any,  # noqa: ANN401
    job: Any,  # noqa: ANN401
    session: Any,  # noqa: ANN401
    github_token: str | None = None,
) -> dict[str, str]:
    """Execute deployment phases in order."""
    from asyncio import to_thread
    from tempfile import mkdtemp

    from app.infrastructure.cluster_health import ClusterHealthChecker
    from app.infrastructure.deployers.factory import DeployerFactory
    from app.infrastructure.git_client import GitClient
    from app.models.cluster import ClusterStatus
    from app.models.deployment import DeploymentStatus, DeployStrategy
    from app.repositories.cluster import ClusterRepository

    cluster_repo = ClusterRepository(session)
    cluster = await cluster_repo.get_by_id(deployment.cluster_id)
    if not cluster or not cluster.kubeconfig:
        msg = "Cluster or kubeconfig not available"
        raise ValueError(msg)

    git_client = GitClient(clone_base_dir=settings.GIT_CLONE_DIR)
    repo_path: Path | None = None

    try:
        # Phase 0: CHECKING_CLUSTER (5%)
        await job_repo.update(
            job,
            status=JobStatus.RUNNING,
            started_at=datetime.now(UTC),
            current_phase="CHECKING_CLUSTER",
            progress_percentage=5,
        )
        await session.commit()

        health_checker = ClusterHealthChecker()
        reachable, health_detail = await health_checker.check_reachable(
            cluster.kubeconfig,
        )
        if not reachable:
            await cluster_repo.update(
                cluster,
                status=ClusterStatus.UNREACHABLE,
                status_message=health_detail,
            )
            await session.commit()
            msg = f"Cluster is unreachable: {health_detail}"
            raise RuntimeError(msg)

        # Phase 1: CLONING_REPO (10%)
        await deployment_repo.update(deployment, status=DeploymentStatus.CLONING)
        await job_repo.update(
            job,
            current_phase="CLONING_REPO",
            progress_percentage=10,
        )
        await session.commit()

        target_dir = f"deploy-{deployment.id}"
        repo_path = await to_thread(
            git_client.clone_repo,
            deployment.repo_url,
            deployment.branch,
            target_dir,
            github_token,
        )

        # Phase 2: VALIDATING_CONFIG (30%)
        await deployment_repo.update(
            deployment,
            status=DeploymentStatus.VALIDATING,
        )
        await job_repo.update(
            job,
            current_phase="VALIDATING_CONFIG",
            progress_percentage=30,
        )
        await session.commit()

        platform_config = await to_thread(
            git_client.parse_platform_config,
            repo_path,
        )

        # Re-clone with correct branch if .platform.yaml specifies a different one
        effective_branch = deployment.branch
        if effective_branch == "main" and platform_config.source.branch != "main":
            effective_branch = platform_config.source.branch
            await to_thread(git_client.cleanup, repo_path)
            repo_path = await to_thread(
                git_client.clone_repo,
                deployment.repo_url,
                effective_branch,
                target_dir,
                github_token,
            )
            platform_config = await to_thread(
                git_client.parse_platform_config,
                repo_path,
            )

        # Update deployment with parsed config info
        await deployment_repo.update(
            deployment,
            strategy=DeployStrategy(platform_config.deploy.strategy.value),
            branch=effective_branch,
            platform_config=platform_config.model_dump(mode="json"),
        )
        await session.commit()

        # Phase 3: DEPLOYING (50%)
        await deployment_repo.update(
            deployment,
            status=DeploymentStatus.DEPLOYING,
        )
        await job_repo.update(
            job,
            current_phase="DEPLOYING",
            progress_percentage=50,
        )
        await session.commit()

        # Write kubeconfig to temp file for deployer
        kubeconfig_tmp = Path(mkdtemp()) / f"kubeconfig-{deployment.id}.yaml"
        kubeconfig_tmp.write_text(cluster.kubeconfig, encoding="utf-8")

        # Create namespace if requested, then deploy
        deployer = DeployerFactory.create(
            deploy_config=platform_config.deploy,
            kubeconfig_path=str(kubeconfig_tmp),
            repo_path=str(repo_path),
            namespace=deployment.namespace,
        )

        if platform_config.cluster.create_namespace:
            await deployer.ensure_namespace()

        deploy_output = await deployer.deploy()

        # Phase 4: VERIFYING (80%)
        await job_repo.update(
            job,
            current_phase="VERIFYING",
            progress_percentage=80,
        )
        await session.commit()

        await deployer.verify()

        # Phase 5: COMPLETE (100%)
        await deployment_repo.update(
            deployment,
            status=DeploymentStatus.COMPLETED,
            status_message="Deployment completed successfully",
            completed_at=datetime.now(UTC),
        )
        await job_repo.update(
            job,
            status=JobStatus.COMPLETED,
            current_phase="COMPLETE",
            progress_percentage=100,
            completed_at=datetime.now(UTC),
            result={"deploy_output": deploy_output[:2000]},
        )
        await session.commit()

        # Cleanup temp kubeconfig
        kubeconfig_tmp.unlink(missing_ok=True)

        return {"status": "completed", "deployment_id": str(deployment.id)}

    finally:
        # Always clean up cloned repo
        if repo_path:
            await to_thread(git_client.cleanup, repo_path)


async def _deploy_application(
    deployment_id: str,
    job_id: str,
    github_token: str | None = None,
) -> dict[str, str]:
    from app.repositories.deployment import DeploymentRepository

    did = uuid.UUID(deployment_id)
    jid = uuid.UUID(job_id)

    async with _make_session_maker()() as session:
        deployment_repo = DeploymentRepository(session)
        job_repo = JobRepository(session)

        deployment = await deployment_repo.get_by_id(did)
        job = await job_repo.get_by_id(jid)
        if not deployment or not job:
            msg = f"Deployment {deployment_id} or Job {job_id} not found"
            raise ValueError(msg)

        try:
            result = await _run_deployment_phases(
                deployment_repo,
                job_repo,
                deployment,
                job,
                session,
                github_token,
            )
        except Exception as exc:
            await session.rollback()
            logger.exception(
                "Application deployment failed",
                extra={"deployment_id": deployment_id, "job_id": job_id},
            )
            await _record_deployment_failure(did, jid, exc)
            raise

    logger.info("Deployment %s completed successfully", deployment_id)
    return result
