"""Celery tasks for application deployment."""

import asyncio
import logging
import uuid
from asyncio import to_thread
from datetime import UTC, datetime
from pathlib import Path
from tempfile import mkdtemp
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.infrastructure.deployers.factory import DeployerFactory
from app.infrastructure.git import GitClient
from app.infrastructure.kubernetes import ClusterHealthChecker
from app.infrastructure.servicemesh.manager import LinkerdManager
from app.models.cluster import ClusterStatus
from app.models.deployment import DeploymentStatus, DeployStrategy
from app.models.job import JobStatus
from app.repositories.cluster import ClusterRepository
from app.repositories.deployment import DeploymentRepository
from app.repositories.job import JobRepository
from app.tasks.celery_config import celery_app
from app.tasks.shared import JobProgress, _make_session_maker, record_job_failure
from app.utils.config import settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Celery task entry point
# ---------------------------------------------------------------------------


@celery_app.task(  # type: ignore[misc]
    bind=True,
    name="app.tasks.delete_deployment",
    max_retries=1,
    soft_time_limit=300,
    time_limit=360,
)
def delete_deployment_task(
    self: Any,  # noqa: ANN401
    deployment_id: str,
    job_id: str,
) -> dict[str, str]:
    """
    Delete a deployment and clean up its K8s resources
    (dispatched by DeploymentService)
    """
    _ = self
    return asyncio.run(_delete_deployment(deployment_id, job_id))


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


# ---------------------------------------------------------------------------
# Async implementations
# ---------------------------------------------------------------------------


async def _record_deployment_failure(
    deployment_id: uuid.UUID,
    job_id: uuid.UUID,
    exc: Exception,
    *,
    prefix: str = "",
) -> None:
    async def _entity(session: AsyncSession) -> None:
        repo = DeploymentRepository(session)
        deployment = await repo.get_by_id(deployment_id)
        if deployment:
            await repo.update(
                deployment,
                status=DeploymentStatus.FAILED,
                status_message=(f"{prefix}{exc!s}" if prefix else str(exc))[:500],
                completed_at=datetime.now(UTC)
            )

    await record_job_failure(job_id, exc, update_entity=_entity)


async def _run_deployment_phases(  # noqa: PLR0913, PLR0915
    deployment_repo: Any,  # noqa: ANN401
    job_repo: JobRepository,
    deployment: Any,  # noqa: ANN401
    job: Any,  # noqa: ANN401
    session: Any,  # noqa: ANN401
    github_token: str | None = None,
) -> dict[str, str]:
    cluster_repo = ClusterRepository(session)
    cluster = await cluster_repo.get_by_id(deployment.cluster_id)
    if not cluster or not cluster.kubeconfig:
        msg = "Cluster or kubeconfig not available"
        raise ValueError(msg)

    git_client = GitClient(clone_base_dir=settings.GIT_CLONE_DIR)
    repo_path: Path | None = None
    progress = JobProgress(job_repo, job)

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
        await progress("CLONING_REPO", 10)

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
        await progress("VALIDATING_CONFIG", 30)

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
        await progress("DEPLOYING", 50)

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

        # Phase 4: VERIFYING (70%)
        await progress("VERIFYING", 70)
        await deployer.verify()

        # Phase 5: INJECT SERVICE MESH (80%)
        await progress("LINKERD_INJECTION", 80)
        linkerd_manager = LinkerdManager(
            gateway_api_version=settings.GATEWAY_API_VERSION,
            kubectl_binary=settings.KUBECTL_BINARY,
            linkerd_binary=settings.LINKERD_BINARY,
        )

        await linkerd_manager.inject_namespaces(
            kubeconfig_path=str(kubeconfig_tmp), namespaces=[deployment.namespace]
        )

        # Phase 6: COMPLETE (100%)
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
        if repo_path:
            await to_thread(git_client.cleanup, repo_path)


async def _deploy_application(
    deployment_id: str,
    job_id: str,
    github_token: str | None = None,
) -> dict[str, str]:

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


async def _delete_deployment(
    deployment_id: str,
    job_id: str,
) -> dict[str, str]:
    did = uuid.UUID(deployment_id)
    jid = uuid.UUID(job_id)

    async with _make_session_maker()() as session:
        deployment_repo = DeploymentRepository(session)
        job_repo = JobRepository(session)
        cluster_repo = ClusterRepository(session)

        deployment = await deployment_repo.get_by_id(did)
        job = await job_repo.get_by_id(jid)
        if not deployment or not job:
            msg = f"Deployment {deployment_id} or Job {job_id} not found"
            raise ValueError(msg)

        try:
            # Phase 1: DELETING_RESOURCES (20%)
            await job_repo.update(
                job,
                status=JobStatus.RUNNING,
                started_at=datetime.now(UTC),
                current_phase="DELETING_RESOURCES",
                progress_percentage=20,
            )
            await session.commit()

            cluster = await cluster_repo.get_by_id(deployment.cluster_id)
            if cluster and cluster.kubeconfig:
                kubeconfig_tmp = Path(mkdtemp()) / f"kubeconfig-{deployment.id}.yaml"
                kubeconfig_tmp.write_text(cluster.kubeconfig, encoding="utf-8")

                try:
                    if deployment.namespace != "default":
                        # Delete the entire namespace (removes all resources within)
                        proc = await asyncio.create_subprocess_exec(
                            "kubectl",
                            "delete",
                            "namespace",
                            deployment.namespace,
                            "--kubeconfig",
                            str(kubeconfig_tmp),
                            "--ignore-not-found",
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE,
                        )
                        _, stderr = await proc.communicate()
                        if proc.returncode != 0:
                            logger.warning(
                                "kubectl delete namespace failed: %s",
                                stderr.decode().strip(),
                            )
                    else:
                        # For default namespace, delete all resources by label/ownership
                        # rather than deleting the namespace itself
                        proc = await asyncio.create_subprocess_exec(
                            "kubectl",
                            "delete",
                            "all",
                            "--all",
                            "-n",
                            "default",
                            "--kubeconfig",
                            str(kubeconfig_tmp),
                            "--ignore-not-found",
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE,
                        )
                        _, stderr = await proc.communicate()
                        if proc.returncode != 0:
                            logger.warning(
                                "kubectl delete all failed: %s",
                                stderr.decode().strip(),
                            )
                finally:
                    kubeconfig_tmp.unlink(missing_ok=True)

            # Phase 2: COMPLETE (100%)
            await deployment_repo.update(
                deployment,
                status=DeploymentStatus.DELETED,
                deleted_at=datetime.now(UTC),
                status_message="Deployment deleted successfully",
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
                "Deployment deletion failed",
                extra={"deployment_id": deployment_id, "job_id": job_id},
            )
            await _record_deployment_failure(
                did,
                jid,
                exc,
                prefix="Deletion failed: ",
            )
            raise

    logger.info("Deployment %s deleted successfully", deployment_id)
    return {"status": "deleted", "deployment_id": deployment_id}
