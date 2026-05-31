import asyncio
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from app.infrastructure.kubernetes import KubernetesVerifier
from app.infrastructure.providers import get_provider
from app.infrastructure.servicemesh.manager import LinkerdManager
from app.models.cluster import ClusterStatus
from app.models.deployment import DeploymentStatus
from app.models.job import JobStatus
from app.repositories.cluster import ClusterRepository
from app.repositories.deployment import DeploymentRepository
from app.repositories.job import JobRepository
from app.tasks.celery_config import celery_app
from app.tasks.shared import _make_session_maker
from app.utils.config import settings
from app.infrastructure.factories import build_litmus_manager, build_monitoring_deployer

logger = logging.getLogger(__name__)


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
    """Provision a cluster using the configured provider."""
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
    """Teardown a cluster using the provider it was created with."""
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
    """Reconnect a cluster using the provider it was created with."""
    _ = self
    return asyncio.run(_reconnect_cluster(cluster_id, job_id))


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
    """Execute provisioning phases via the cluster provider."""
    config = cluster.ports or {}
    provider_type = config.get("provider", settings.CLUSTER_PROVIDER)
    provider = get_provider(provider_type, cluster_repo=cluster_repo)
    verifier = KubernetesVerifier()

    await cluster_repo.update(cluster, status=ClusterStatus.PROVISIONING)
    await job_repo.update(
        job,
        status=JobStatus.RUNNING,
        started_at=datetime.now(UTC),
    )
    await session.commit()

    async def on_progress(phase: str, pct: int) -> None:
        await job_repo.update(job, current_phase=phase, progress_percentage=pct)
        await session.commit()

    # Provider handles infra-specific phases (5-55%)
    result = await provider.provision(cluster.kind_name, config, on_progress)

    # Phase: VERIFYING (60%)
    await on_progress("VERIFYING", 60)
    await verifier.verify_cluster_ready(result.kubeconfig_content)

    # Phase: DEPLOYING_MONITORING (70%)
    await on_progress("DEPLOYING_MONITORING", 70)
    
    
    monitoring_deployer = build_monitoring_deployer()
    vm_url = await monitoring_deployer.deploy(
        result.kubeconfig_content,
        result.control_plane_ip,
    )
    await cluster_repo.update(cluster, victoriametrics_url=vm_url)
    await session.commit()

    # Phase: DEPLOYING_LINKERD (80%)
    await on_progress("DEPLOYING_LINKERD", 80)
    linkerd_manager = LinkerdManager(
        gateway_api_version=settings.GATEWAY_API_VERSION,
        kubectl_binary=settings.KUBECTL_BINARY,
        linkerd_binary=settings.LINKERD_BINARY,
    )
    await linkerd_manager.deploy(kubeconfig_content=result.kubeconfig_content)

    # Phase: DEPLOYING_LITMUS (85%)
    await on_progress("DEPLOYING_LITMUS", 85)
    litmus_manager = build_litmus_manager()
    await litmus_manager.deploy(kubeconfig_content=result.kubeconfig_content)

    # Phase: COMPLETE (100%)
    await cluster_repo.update(
        cluster,
        status=ClusterStatus.READY,
        kubeconfig=result.kubeconfig_content,
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

            config = cluster.ports or {}
            provider_type = config.get("provider", settings.CLUSTER_PROVIDER)
            provider = get_provider(provider_type)
            await provider.teardown(cluster.kind_name, config)

            # Soft-delete all active deployments on this cluster
            deployment_repo = DeploymentRepository(session)
            active_deployments = await deployment_repo.list_by_cluster(cid)
            for dep in active_deployments:
                await deployment_repo.update(
                    dep,
                    status=DeploymentStatus.DELETED,
                    deleted_at=datetime.now(UTC),
                    status_message="Deleted due to cluster teardown",
                )

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
                current_phase="CHECK_INFRASTRUCTURE",
                progress_percentage=10,
            )
            await session.commit()

            config = cluster.ports or {}
            provider_type = config.get("provider", settings.CLUSTER_PROVIDER)
            provider = get_provider(provider_type)

            try:
                result = await provider.reconnect(cluster.kind_name, config)
            except RuntimeError as exc:
                await cluster_repo.update(
                    cluster,
                    status=ClusterStatus.FAILED,
                    status_message=str(exc)[:500],
                )
                await job_repo.update(
                    job,
                    status=JobStatus.FAILED,
                    current_phase="CHECK_INFRASTRUCTURE",
                    error_message=str(exc)[:1000],
                    completed_at=datetime.now(UTC),
                )
                await session.commit()
                return {
                    "status": "failed",
                    "cluster_id": cluster_id,
                    "reason": "infrastructure_not_found",
                }

            # Phase: VERIFYING (75%)
            await job_repo.update(
                job,
                current_phase="VERIFYING",
                progress_percentage=75,
            )
            await session.commit()

            verifier = KubernetesVerifier()
            await verifier.verify_cluster_ready(
                result.kubeconfig_content,
                timeout_seconds=60,
            )

            # Phase: COMPLETE (100%)
            await cluster_repo.update(
                cluster,
                status=ClusterStatus.READY,
                kubeconfig=result.kubeconfig_content,
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
