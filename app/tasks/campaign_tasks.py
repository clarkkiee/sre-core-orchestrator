import asyncio
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from app.infrastructure.chaos.discovery import discover_services
from app.infrastructure.chaos.manager import LitmusChaosManager
from app.infrastructure.chaos.manifests import (
    _NEEDS_RUNTIME_SOCKET,
    EXPERIMENT_TEMPLATES,
)
from app.infrastructure.chaos.probes import build_probes
from app.models.campaign import CampaignStatus, ChaosCampaign
from app.models.chaos import ChaosExperiment, ChaosExperimentStatus, ExperimentType
from app.models.job import Job, JobStatus
from app.repositories.campaign import CampaignRepository
from app.repositories.chaos import ChaosRepository
from app.repositories.cluster import ClusterRepository
from app.repositories.deployment import DeploymentRepository
from app.repositories.job import JobRepository
from app.services.evaluation import RECOVERY_WINDOW_SECONDS
from app.tasks.celery_config import celery_app
from app.tasks.chaos_tasks import (
    _EOT_PROBE_ATTEMPT_TIMEOUT,
    _PRE_CHAOS_OVERHEAD,
    _fetch_chaos_injected_time,
)
from app.tasks.shared import _make_session_maker
from app.utils.config import settings

logger = logging.getLogger(__name__)

# Maps litmus experiment name → model enum value.
_LITMUS_TO_TYPE: dict[str, ExperimentType] = {
    "pod-delete": ExperimentType.POD_DELETE,
    "pod-cpu-hog": ExperimentType.POD_CPU_HOG,
    "pod-memory-hog": ExperimentType.POD_MEMORY_HOG,
    "pod-network-latency": ExperimentType.POD_NETWORK_LATENCY,
    "pod-network-loss": ExperimentType.POD_NETWORK_LOSS,
}


# ---------------------------------------------------------------------------
# Celery task entry point
# ---------------------------------------------------------------------------


@celery_app.task(  # type: ignore[misc]
    bind=True,
    name="app.tasks.run_chaos_campaign",
    max_retries=1,
)
def run_chaos_campaign_task(
    self: Any,  # noqa: ANN401
    campaign_id: str,
    job_id: str,
) -> dict[str, str]:
    """Run a full Chaos Campaign against all discovered services."""
    _ = self
    return asyncio.run(
        _run_chaos_campaign(campaign_id=campaign_id, job_id=job_id)
    )


# ---------------------------------------------------------------------------
# Async implementation
# ---------------------------------------------------------------------------


async def _record_campaign_failure(
    campaign_id: uuid.UUID,
    job_id: uuid.UUID,
    exc: Exception,
) -> None:
    """Record a campaign-level failure in a fresh DB session."""
    async with _make_session_maker()() as session:
        c_repo = CampaignRepository(session)
        j_repo = JobRepository(session)
        campaign = await c_repo.get_by_id(campaign_id)
        job = await j_repo.get_by_id(job_id)
        if campaign:
            await c_repo.update(
                campaign,
                status=CampaignStatus.FAILED,
                status_message=str(exc)[:500],
                completed_at=datetime.now(UTC),
            )
        if job:
            await j_repo.update(
                job,
                status=JobStatus.FAILED,
                error_message=str(exc)[:1000],
                completed_at=datetime.now(UTC),
            )
        await session.commit()


async def _run_single_experiment(  # noqa: PLR0913
    litmus_manager: LitmusChaosManager,
    chaos_repo: ChaosRepository,
    kubeconfig: str,
    experiment: ChaosExperiment,
    litmus_name: str,
    target_port: int,
    service_protocol: str,
    target_clusterip: str,
    session: Any,  # noqa: ANN401
    vm_url: str | None = None,
) -> str:
    """Execute one chaos experiment and return the verdict.

    Creates the ChaosEngine, polls for the result, records it, then cleans up.
    """
    engine_name = experiment.chaos_engine_name
    if not engine_name:
        msg = "chaos_engine_name is missing"
        raise ValueError(msg)

    try:
        await chaos_repo.update(
            experiment,
            status=ChaosExperimentStatus.RUNNING,
            started_at=datetime.now(UTC),
        )

        # BASELINE PHASE
        baseline_start = datetime.now(UTC)
        await asyncio.sleep(RECOVERY_WINDOW_SECONDS)
        baseline_end = datetime.now(UTC)

        deployment_repo = DeploymentRepository(session)
        deployment = await deployment_repo.get_by_id(experiment.deployment_id)
        probes = build_probes(
            experiment_type=litmus_name,
            namespace=experiment.target_namespace,
            target_label=experiment.target_label,
            target_port=target_port,
            service_protocol=service_protocol,
            target_clusterip=target_clusterip,
            prom_url=vm_url,
            settings=settings,
            deployment_thresholds=(
                deployment.probe_thresholds if deployment else None
            ),
            experiment_configuration=experiment.configuration,
        )
        
        # FAULT PHASE
        await litmus_manager.create_experiment(
            kubeconfig_content=kubeconfig,
            namespace=experiment.target_namespace,
            engine_name=engine_name,
            experiment_type=litmus_name,
            app_label=experiment.target_label,
            duration=experiment.duration_seconds,
            configuration=experiment.configuration,
            probes=probes,
        )
        await session.commit()

        _eot_overhead = (
            settings.PROBE_DEFAULT_RECOVERY_INITIAL_DELAY_S
            + _EOT_PROBE_ATTEMPT_TIMEOUT
            + settings.PROBE_DEFAULT_RECOVERY_RETRY * (
                settings.PROBE_DEFAULT_RECOVERY_INTERVAL_S + _EOT_PROBE_ATTEMPT_TIMEOUT
            )
        )
        timeout = experiment.duration_seconds + _eot_overhead + _PRE_CHAOS_OVERHEAD
        chaos_result = await litmus_manager.poll_experiment_result(
            kubeconfig_content=kubeconfig,
            engine_name=engine_name,
            experiment_type=litmus_name,
            namespace=experiment.target_namespace,
            timeout=timeout,
        )

        # Capture chaos_injected_time immediately after fault completes,
        # while the exporter still points at this engine.
        logger.info("START FETCH CHAOS INJECTED TIME")
        injected_time = await _fetch_chaos_injected_time(
            vm_url=vm_url,
            engine_name=engine_name,
        )
        logger.info("END FETCH CHAOS INJECTED TIME")

        # RECOVERY PHASE
        recovery_start = datetime.now(UTC)
        await asyncio.sleep(RECOVERY_WINDOW_SECONDS)
        recovery_end = datetime.now(UTC)

        verdict = (
            chaos_result.get("status", {})
            .get("experimentStatus", {})
            .get("verdict", "N/A")
        )

        # Re-sync identity map after long sleeps (baseline + fault + recovery).
        await session.refresh(experiment)
        await chaos_repo.update(
            experiment,
            result=chaos_result,
            status=ChaosExperimentStatus.COMPLETED,
            status_message=f"Verdict: {verdict}",
            completed_at=datetime.now(UTC),
            chaos_injected_time=injected_time,
            baseline_start=baseline_start.timestamp(),
            baseline_end=baseline_end.timestamp(),
            recovery_start=recovery_start.timestamp(),
            recovery_end=recovery_end.timestamp(),
        )
        await session.commit()

    except Exception as exc:
        logger.exception(
            "Experiment %s (%s) failed", experiment.id, litmus_name,
        )
        await chaos_repo.update(
            experiment,
            status=ChaosExperimentStatus.FAILED,
            status_message=str(exc)[:500],
            completed_at=datetime.now(UTC),
        )
        await session.commit()
        return "Error"
    else:
        # Enqueue evaluation via celery by task name to avoid static type issues
        celery_app.send_task("app.tasks.evaluate_experiment", args=[str(experiment.id)])
        logger.info("Enqueued evaluation for experiment=%s", experiment.id)
        return verdict
    finally:
        # Cleanup: delete the ChaosEngine CR regardless of outcome.
        try:
            await litmus_manager.delete_experiment(
                kubeconfig_content=kubeconfig,
                engine_name=engine_name,
                namespace=experiment.target_namespace,
            )
        except Exception:
            logger.warning("Failed to cleanup engine %s", engine_name, exc_info=True)


async def _run_campaign_phases(  # noqa: PLR0913, PLR0915
    campaign_repo: CampaignRepository,
    chaos_repo: ChaosRepository,
    job_repo: JobRepository,
    job: Job,
    session: Any,  # noqa: ANN401
    campaign: ChaosCampaign,
    kubeconfig: str,
    vm_url: str | None = None,
) -> dict[str, str]:
    """Execute all campaign phases in order."""
    litmus_manager = LitmusChaosManager(
        kubectl_binary=settings.KUBECTL_BINARY,
        litmus_version=settings.LITMUS_VERSION,
        litmus_runner_image=settings.LITMUS_RUNNER_IMAGE,
    )

    # ------------------------------------------------------------------
    # Phase 1: VALIDATING (2%)
    # ------------------------------------------------------------------
    await job_repo.update(
        job, status=JobStatus.RUNNING, current_phase="VALIDATING", progress_percentage=2
    )
    await session.commit()

    is_ready = await litmus_manager.verify_operator(kubeconfig)
    if not is_ready:
        msg = "Litmus Operator is not running"
        raise RuntimeError(msg)

    # ------------------------------------------------------------------
    # Phase 2: DISCOVERING (5%)
    # ------------------------------------------------------------------
    await job_repo.update(job, current_phase="DISCOVERING", progress_percentage=5)
    await campaign_repo.update(campaign, status=CampaignStatus.DISCOVERING)
    await session.commit()

    services = await discover_services(kubeconfig, campaign.target_namespace)
    if not services:
        msg = (
            f"No application services discovered in namespace "
            f"{campaign.target_namespace}"
        )
        raise RuntimeError(msg)

    experiment_types = list(EXPERIMENT_TEMPLATES.keys())
    total = len(experiment_types) * len(services)

    await campaign_repo.update(
        campaign,
        discovered_services=services,
        total_experiments=total,
        status=CampaignStatus.RUNNING,
        started_at=datetime.now(UTC),
    )
    await session.commit()

    logger.info(
        "Campaign %s: discovered %d services, %d total experiments",
        campaign.id, len(services), total,
    )

    # ------------------------------------------------------------------
    # Phase 3: PREPARING (10%)
    # ------------------------------------------------------------------
    await job_repo.update(job, current_phase="PREPARING_RBAC", progress_percentage=10)
    await session.commit()

    await litmus_manager.setup_experiment_rbac(
        kubeconfig_content=kubeconfig, namespace=campaign.target_namespace
    )

    # ------------------------------------------------------------------
    # Phase 4: EXECUTING (10% → 90%)
    # ------------------------------------------------------------------
    completed = 0
    failed_count = 0

    for litmus_name in experiment_types:
        experiment_type_enum = _LITMUS_TO_TYPE[litmus_name]

        for service in services:
            # Check if campaign was stopped between experiments.
            await session.refresh(campaign)
            if campaign.status == CampaignStatus.STOPPED:
                logger.info("Campaign %s stopped by user", campaign.id)
                await job_repo.update(
                    job,
                    current_phase="STOPPED",
                    status=JobStatus.CANCELED,
                    completed_at=datetime.now(UTC),
                )
                await session.commit()
                return {"campaign_id": str(campaign.id), "status": "STOPPED"}

            engine_name = (
                f"{litmus_name}-{service['name']}-{uuid.uuid4().hex[:8]}"
            )

            # Build configuration for this experiment.
            config: dict[str, str] | None = None
            if litmus_name in _NEEDS_RUNTIME_SOCKET:
                config = {"TARGET_CONTAINER": service["container"]}

            # Determine default duration from the template.
            template_env = EXPERIMENT_TEMPLATES[litmus_name]["env"]
            duration = int(template_env.get("TOTAL_CHAOS_DURATION", "60"))

            experiment = ChaosExperiment(
                tenant_id=campaign.tenant_id,
                cluster_id=campaign.cluster_id,
                deployment_id=campaign.deployment_id,
                campaign_id=campaign.id,
                experiment_type=experiment_type_enum,
                target_namespace=campaign.target_namespace,
                target_label=service["label"],
                duration_seconds=duration,
                chaos_engine_name=engine_name,
                configuration=config,
                status=ChaosExperimentStatus.PENDING,
            )
            experiment = await chaos_repo.create(experiment)
            await session.commit()

            logger.info(
                "Campaign %s: running %s → %s (%d/%d)",
                campaign.id, litmus_name, service["name"], completed + 1, total,
            )

            await job_repo.update(
                job,
                current_phase=f"BASELINE_WAIT:{litmus_name}:{service['name']}",
                progress_percentage=10 + int(80 * completed / total),
            )
            await session.commit()

            verdict = await _run_single_experiment(
                litmus_manager=litmus_manager,
                chaos_repo=chaos_repo,
                kubeconfig=kubeconfig,
                experiment=experiment,
                litmus_name=litmus_name,
                target_port=int(service["port"]),
                service_protocol=str(service.get("protocol", "http")),
                target_clusterip=str(service.get("clusterIP", "")),
                session=session,
                vm_url=vm_url,
            )

            completed += 1
            if verdict == "Error":
                failed_count += 1

            await campaign_repo.update(
                campaign, completed_experiments=completed
            )
            await session.commit()

    # ------------------------------------------------------------------
    # Phase 5: COMPLETE (100%)
    # ------------------------------------------------------------------
    final_status = (
        CampaignStatus.FAILED if failed_count == total else CampaignStatus.COMPLETED
    )
    status_msg = (
        f"Completed: {completed - failed_count} passed, {failed_count} failed"
    )

    await campaign_repo.update(
        campaign,
        status=final_status,
        status_message=status_msg,
        completed_at=datetime.now(UTC),
    )
    await job_repo.update(
        job,
        current_phase="COMPLETE",
        progress_percentage=100,
        status=JobStatus.COMPLETED,
        completed_at=datetime.now(UTC),
    )
    await session.commit()

    return {"campaign_id": str(campaign.id), "status": final_status.value}


async def _run_chaos_campaign(
    campaign_id: str, job_id: str
) -> dict[str, str]:
    cid = uuid.UUID(campaign_id)
    jid = uuid.UUID(job_id)

    async with _make_session_maker()() as session:
        campaign_repo = CampaignRepository(session)
        chaos_repo = ChaosRepository(session)
        job_repo = JobRepository(session)
        cluster_repo = ClusterRepository(session)

        campaign = await campaign_repo.get_by_id(cid)
        job = await job_repo.get_by_id(jid)
        if not campaign or not job:
            msg = f"Campaign {campaign_id} or Job {job_id} not found"
            raise ValueError(msg)

        cluster = await cluster_repo.get_by_id(campaign.cluster_id)
        if not cluster:
            msg = f"Cluster {campaign.cluster_id} not found"
            raise ValueError(msg)

        kubeconfig = cluster.kubeconfig
        if not kubeconfig:
            msg = "kubeconfig is missing"
            raise ValueError(msg)

        try:
            result = await _run_campaign_phases(
                campaign_repo=campaign_repo,
                chaos_repo=chaos_repo,
                job_repo=job_repo,
                job=job,
                session=session,
                campaign=campaign,
                kubeconfig=kubeconfig,
                vm_url=cluster.victoriametrics_url,
            )
        except Exception as exc:
            await session.rollback()
            logger.exception(
                "Chaos campaign failed",
                extra={"campaign_id": campaign_id, "job_id": job_id},
            )
            await _record_campaign_failure(cid, jid, exc)
            raise

        logger.info("Chaos campaign %s finished", campaign_id)
        return result
