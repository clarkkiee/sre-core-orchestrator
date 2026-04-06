import asyncio
import logging
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.infrastructure.chaos.manager import LitmusChaosManager
from app.infrastructure.kind import KindClient, KindCommandError
from app.infrastructure.kind.config_builder import KindConfigBuilder
from app.infrastructure.kubernetes import KubernetesVerifier
from app.infrastructure.metrics.deployer import MonitoringStackDeployer
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

    # Phase 1: BUILDING_CONFIG (10%)
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

    # Phase 2: CREATING_CLUSTER (25%)
    await job_repo.update(
        job,
        current_phase="CREATING_CLUSTER",
        progress_percentage=25,
    )
    await session.commit()

    if not await kind_client.cluster_exists(cluster.kind_name):
        await kind_client.create_cluster(cluster.kind_name, str(config_path))

    # Phase 3: EXPORTING_KUBECONFIG (45%)
    await job_repo.update(
        job,
        current_phase="EXPORTING_KUBECONFIG",
        progress_percentage=45,
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

    # Phase 4: VERIFYING (60%)
    await job_repo.update(
        job,
        current_phase="VERIFYING",
        progress_percentage=60,
    )
    await session.commit()

    await verifier.verify_cluster_ready(kubeconfig_content)

    # Phase 5: DEPLOYING_MONITORING (70%)
    await job_repo.update(
        job,
        current_phase="DEPLOYING_MONITORING",
        progress_percentage=70,
    )
    await session.commit()

    monitoring_deployer = MonitoringStackDeployer(
        vm_image=settings.VM_IMAGE,
        ksm_image=settings.KSM_IMAGE,
        vm_nodeport=settings.VM_NODEPORT,
        bbe_image=settings.BLACKBOX_IMAGE,
    )
    vm_url = await monitoring_deployer.deploy(
        kubeconfig_content, cp_ip, kind_name=cluster.kind_name
    )
    await cluster_repo.update(cluster, victoriametrics_url=vm_url)
    await session.commit()

    # Phase 6: DEPLOYING SERVICE MESH (LINKERD) (80%)
    await job_repo.update(
        job,
        current_phase="DEPLOYING_LINKERD",
        progress_percentage=80,
    )
    await session.commit()

    linkerd_manager = LinkerdManager(
        gateway_api_version=settings.GATEWAY_API_VERSION,
        kubectl_binary=settings.KUBECTL_BINARY,
        linkerd_binary=settings.LINKERD_BINARY,
    )

    await linkerd_manager.deploy(kubeconfig_content=kubeconfig_content)

    # Phase 7: DEPLOYING_LITMUS (85%)
    await job_repo.update(
        job,
        current_phase="DEPLOYING_LITMUS",
        progress_percentage=85,
    )
    await session.commit()

    litmus_manager = LitmusChaosManager(
        kubectl_binary=settings.KUBECTL_BINARY, litmus_version=settings.LITMUS_VERSION
    )

    await litmus_manager.deploy(kubeconfig_content=kubeconfig_content)

    # Phase 6: COMPLETE (100%)
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
