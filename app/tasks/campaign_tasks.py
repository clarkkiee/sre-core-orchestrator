import asyncio
import logging
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from app.infrastructure.chaos.discovery import discover_services
from app.infrastructure.chaos.exceptions import ClusterNotReadyError
from app.infrastructure.chaos.manager import LitmusChaosManager
from app.infrastructure.chaos.probes import (
    build_probes,
    estimate_eot_probe_overhead_seconds,
)
from app.tasks.chaos_tasks import _fetch_chaos_injected_time
from app.infrastructure.metrics.client import VictoriaMetricsClient
from app.infrastructure.metrics.catalog import MetricCatalog
from app.services.evaluation import extract_baseline_metrics
from app.infrastructure.chaos.probes import derive_thresholds_from_baseline
from app.models.campaign import CampaignStatus, ChaosCampaign
from app.models.chaos import ChaosExperiment, ChaosExperimentStatus, ExperimentType
from app.models.job import Job, JobStatus
from app.repositories.campaign import CampaignRepository
from app.repositories.chaos import ChaosRepository
from app.repositories.cluster import ClusterRepository
from app.repositories.deployment import DeploymentRepository
from app.repositories.job import JobRepository
from app.services.evaluation import EVALUATION_WINDOW_SECONDS
from app.tasks.celery_config import celery_app
from app.tasks.chaos_tasks import _POST_CHAOS_OVERHEAD, _PRE_CHAOS_OVERHEAD
from app.tasks.shared import _make_session_maker
from app.utils.config import settings
from app.infrastructure.chaos import experiments
from app.infrastructure.config_values import load_values

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
    target_port: str,
    service_protocol: str,
    target_clusterip: str,
    session: Any,  # noqa: ANN401
    vm_url: str | None = None,
    health_path: str | None = None,
) -> str:
    """Execute one chaos experiment and return the verdict.

    Creates the ChaosEngine, polls for the result, records it, then cleans up.
    """
    engine_name = experiment.chaos_engine_name
    if not engine_name:
        msg = "chaos_engine_name is missing"
        raise ValueError(msg)

    try:
        # PRE BASELINE CLUSTER CEK
        await litmus_manager.ensure_cluster_ready(
            kubeconfig_content=kubeconfig,
            namespace=experiment.target_namespace,
            target_label=experiment.target_label,
            timeout_s=settings.CLUSTER_READY_TIMEOUT_S,
            interval_s=settings.CLUSTER_READY_INTERVAL_S,
            require_no_active_engine=settings.CLUSTER_READY_REQUIRE_NO_ACTIVE_ENGINE,
        )
        
        await chaos_repo.update(
            experiment,
            status=ChaosExperimentStatus.RUNNING,
            started_at=datetime.now(UTC),
        )
        await session.commit()
        
        # BASELINE PHASE
        baseline_start = datetime.now(UTC)
        logger.info(
            "Baseline phase: experiment=%s observing for %ds",
            experiment.id, EVALUATION_WINDOW_SECONDS
        )
        await asyncio.sleep(EVALUATION_WINDOW_SECONDS)
        baseline_end = datetime.now(UTC)
        
        # EXTRACT BASELINE METRICS
        derived_thresholds: dict[str, Any] = {}
        baseline_metrics: dict[str, float | None] = {}

        if settings.BASELINE_METRICS_ENABLED and vm_url:
            vm_client = VictoriaMetricsClient(vm_url)
            catalog = MetricCatalog.load_from_dir()
            
            baseline_metrics = await extract_baseline_metrics(
                baseline_start=baseline_start,
                baseline_end=baseline_end,
                catalog=catalog,
                namespace=experiment.target_namespace,
                target_label=experiment.target_label,
                vm_client=vm_client
            )
            
            derived_thresholds = derive_thresholds_from_baseline(
                baseline_metrics=baseline_metrics,
                settings=settings
            )
            
            logger.info(
                "Baseline derived probe thresholds for experiment=%s: %s",
                experiment.id, derived_thresholds,
            )
            
        # Build probes with derived thresholds
        deployment_repo = DeploymentRepository(session)
        deployment = await deployment_repo.get_by_id(experiment.deployment_id)
        deployment_thresholds = deployment.probe_thresholds if deployment and deployment.probe_thresholds else {}
        if health_path:
            deployment_thresholds["target_health_path"] = health_path
        
        experiment_configuration = dict(experiment.configuration or {})
        probes_cfg = dict(experiment_configuration.get("probes") or {})
        existing_thresholds = dict(probes_cfg.get("thresholds") or {})
        
        merged_thresholds = {**derived_thresholds, **existing_thresholds}
        probes_cfg["thresholds"] = merged_thresholds
        experiment_configuration["probes"] = probes_cfg

        probes = build_probes(
            experiment_type=litmus_name,
            namespace=experiment.target_namespace,
            target_label=experiment.target_label,
            target_port=target_port,
            service_protocol=service_protocol,
            target_clusterip=target_clusterip,
            prom_url=vm_url,
            settings=settings,
            deployment_thresholds=deployment_thresholds,
            experiment_configuration=experiment_configuration,
        )
        
        await chaos_repo.update(
            experiment,
            baseline_metrics=baseline_metrics,
            baseline_start=baseline_start.timestamp(),
            baseline_end=baseline_end.timestamp(),
        )
        await session.commit()
        
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

        eot_overhead = estimate_eot_probe_overhead_seconds(probes)
        timeout = (
            experiment.duration_seconds
            + eot_overhead
            + _PRE_CHAOS_OVERHEAD
            + _POST_CHAOS_OVERHEAD
        )
        chaos_result = await litmus_manager.poll_experiment_result(
            kubeconfig_content=kubeconfig,
            engine_name=engine_name,
            experiment_type=litmus_name,
            namespace=experiment.target_namespace,
            timeout=timeout,
        )

        injected_time = await _fetch_chaos_injected_time(
            vm_url=vm_url,
            engine_name=engine_name,
        )

        if injected_time is None:
            msg = f"Failed to fetch Chaos Injected Time"
            raise ValueError(msg)
        
        # RECOVERY PHASE
        fault_start = datetime.fromtimestamp(injected_time, tz=UTC)
        fault_end = fault_start + timedelta(seconds=experiment.duration_seconds)
      
        recovery_start = fault_end
        recovery_end = fault_end + timedelta(seconds=EVALUATION_WINDOW_SECONDS)
        
        now = datetime.now(UTC)
        if now < recovery_end:
            await asyncio.sleep((recovery_end - now).total_seconds())

        verdict = (
            chaos_result.get("status", {})
            .get("experimentStatus", {})
            .get("verdict", "N/A")
        )

        await session.refresh(experiment)
        await chaos_repo.update(
            experiment,
            result=chaos_result,
            status=ChaosExperimentStatus.COMPLETED,
            status_message=f"Verdict: {verdict}",
            completed_at=datetime.now(UTC),
            recovery_start=recovery_start.timestamp(),
            recovery_end=recovery_end.timestamp(),
            chaos_injected_time=injected_time
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
        
        try:
            await litmus_manager.ensure_cluster_ready(
                kubeconfig_content=kubeconfig,
                namespace=experiment.target_namespace,
                target_label=experiment.target_label,
                interval_s=settings.CLUSTER_READY_INTERVAL_S,
                timeout_s=settings.CLUSTER_READY_TIMEOUT_S,
                require_no_active_engine=settings.CLUSTER_READY_REQUIRE_NO_ACTIVE_ENGINE
            )
        except ClusterNotReadyError as e:
            logger.warning(
                "Post recovery cluster readiness check failed for experiment=%s: ",
                experiment.id, e
            )
            await chaos_repo.update(
                experiment,
                status_message=f"Verdict: {verdict} (post-recovery): not_ready: {e.reason}"
            )
            await session.commit()
        
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
    
    values = load_values()
    
    litmus_manager = LitmusChaosManager(
        kubectl_binary=settings.KUBECTL_BINARY,
        litmus_version=values["litmus"]["version"],
        litmus_runner_image=values["litmus"]["runner_image"],
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

    experiment_types = experiments.experiment_names()
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
            # Jika campaign dihentikan, early stop
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
            if experiments.needs_runtime_socket(litmus_name):
                config = {"TARGET_CONTAINER": service["container"]}

            # Determine default duration from the template.
            template_env = experiments.get_experiment(litmus_name)["env"]
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
                target_port=str(
                    service.get("port", settings.PROBE_DEFAULT_TARGET_PORT)
                ),
                service_protocol=str(service.get("protocol", "http")),
                target_clusterip=str(service.get("clusterIP", "")),
                session=session,
                vm_url=vm_url,
                health_path=service.get("health_path"),
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
