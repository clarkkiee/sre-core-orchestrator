import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.infrastructure.chaos.discovery import (
    resolve_service_target,
)
from app.infrastructure.chaos.exceptions import ClusterNotReadyError
from app.infrastructure.chaos.manager import LitmusChaosManager
from app.infrastructure.chaos.naming import TYPE_TO_LITMUS_NAME
from app.infrastructure.chaos.probes import (
    ProbeBuildContext,
    build_probes,
    derive_thresholds_from_baseline,
    estimate_eot_probe_overhead_seconds,
)
from app.infrastructure.factories import build_litmus_manager
from app.infrastructure.metrics.catalog import MetricCatalog
from app.infrastructure.metrics.client import VictoriaMetricsClient
from app.models.chaos import ChaosExperiment, ChaosExperimentStatus
from app.models.cluster import Cluster
from app.models.job import Job, JobStatus
from app.repositories.chaos import ChaosRepository
from app.repositories.cluster import ClusterRepository
from app.repositories.deployment import DeploymentRepository
from app.repositories.job import JobRepository
from app.services.evaluation import EVALUATION_WINDOW_SECONDS, extract_baseline_metrics
from app.tasks.celery_config import celery_app
from app.tasks.shared import JobProgress, _make_session_maker, record_job_failure
from app.utils.config import settings

logger = logging.getLogger(__name__)

_INJECTED_TIME_POLL_TIMEOUT = 60.0
_INJECTED_TIME_POLL_INTERVAL = 5.0

# Runner pod scheduling + container startup + SOT probe execution.
_PRE_CHAOS_OVERHEAD = 35
_POST_CHAOS_OVERHEAD = 60


@dataclass
class BaselineResult:
    metrics: dict[str, float | None]
    derived_thresholds: dict[str, Any]
    start: datetime
    end: datetime


# ---------------------------------------------------------------------------
# Celery task entry point
# ---------------------------------------------------------------------------


@celery_app.task(  # type: ignore[misc]
    bind=True,
    name="app.tasks.run_chaos_experiment",
    max_retries=1,
)
def run_chaos_experiment_task(
    self: Any,  # noqa: ANN401
    experiment_id: str,
    job_id: str,
) -> dict[str, str]:
    """Run single Chaos Experiment into a targeted AUT (dispatched by ChaosService)"""
    _ = self
    return asyncio.run(
        _run_chaos_experiment(experiment_id=experiment_id, job_id=job_id)
    )


# ---------------------------------------------------------------------------
# Async implementations
# ---------------------------------------------------------------------------


async def _record_chaos_experiment_failure(
    experiment_id: uuid.UUID,
    job_id: uuid.UUID,
    exc: Exception,
    *,
    prefix: str = "",
) -> None:
    async def _entity(session: AsyncSession) -> None:
        repo = ChaosRepository(session)
        exp = await repo.get_by_id(experiment_id)
        if exp:
            msg = f"{prefix}{exc!s}" if prefix else str(exc)
            await repo.update(
                exp, status=ChaosExperimentStatus.FAILED,
                status_message=msg[:500]
            )
    await record_job_failure(job_id, exc, update_entity=_entity)


async def _fetch_chaos_injected_time(
    vm_url: str | None,
    engine_name: str,
) -> float | None:
    """Query VM for the live chaos_injected_time gauge while the engine is still active."""
    if not vm_url or not engine_name:
        return None

    client = VictoriaMetricsClient(vm_url)
    promql = (
        f'litmuschaos_experiment_chaos_injected_time'
        f'{{chaosengine_name="{engine_name}"}}'
    )

    deadline = asyncio.get_running_loop().time() + _INJECTED_TIME_POLL_TIMEOUT
    attempt = 0

    while True:
        attempt += 1
        try:
            resp = await client.instant_query(promql)
            value = client.extract_scalar(resp)
            if value and value > 0:
                logger.info(
                    "Captured chaos_injected_time=%.0f for engine=%s on attempt=%d",
                    value, engine_name, attempt,
                )
                return value
        except Exception:
            logger.warning(
                "chaos_injected_time poll attempt=%d failed for engine=%s",
                attempt, engine_name, exc_info=True,
            )

        if asyncio.get_running_loop().time() >= deadline:
            logger.warning(
                "chaos_injected_time not available within %.0fs for engine=%s "
                "(attempts=%d) — relying on evaluation fallback",
                _INJECTED_TIME_POLL_TIMEOUT, engine_name, attempt,
            )
            return None

        await asyncio.sleep(_INJECTED_TIME_POLL_INTERVAL)


async def _collect_baseline(
    experiment: ChaosExperiment,
    vm_url: str | None
) -> BaselineResult:
    start = datetime.now(UTC)
    logger.info(
        "Baseline phase: experiment=%s observing for %ds",
        experiment.id, EVALUATION_WINDOW_SECONDS
    )
    await asyncio.sleep(EVALUATION_WINDOW_SECONDS)
    end = datetime.now(UTC)

    metrics: dict[str, float | None] = {}
    derived: dict[str, Any] = {}

    if settings.BASELINE_METRICS_ENABLED and vm_url:
        vm_client = VictoriaMetricsClient(vm_url)
        catalog = MetricCatalog.load_from_dir()
        metrics = await extract_baseline_metrics(
            vm_client=vm_client, catalog=catalog,
            baseline_start=start, baseline_end=end,
            namespace=experiment.target_namespace,
            target_label=experiment.target_label
        )
        derived = derive_thresholds_from_baseline(metrics, settings)
        logger.info("Baseline metrics derived thresholds for experiment=%s: %s", experiment.id, derived)

    return BaselineResult(metrics=metrics, derived_thresholds=derived, start=start, end=end)


async def _run_single_experiment(  # noqa: PLR0913,PLR0915
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
    on_phase: Callable[[str], Awaitable[None]] | None = None,
) -> str:
    async def _report(phase: str) -> None:
        if on_phase is not None:
            await on_phase(phase)

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
        await _report("BASELINE")
        baseline = await _collect_baseline(experiment, vm_url)

        # Build probes with derived thresholds
        deployment_repo = DeploymentRepository(session)
        deployment = await deployment_repo.get_by_id(experiment.deployment_id)
        deployment_thresholds = deployment.probe_thresholds if deployment and deployment.probe_thresholds else {}
        if health_path:
            deployment_thresholds["target_health_path"] = health_path

        experiment_configuration = dict(experiment.configuration or {})
        probes_cfg = dict(experiment_configuration.get("probes") or {})
        existing_thresholds = dict(probes_cfg.get("thresholds") or {})

        merged_thresholds = {**baseline.derived_thresholds, **existing_thresholds}
        probes_cfg["thresholds"] = merged_thresholds
        experiment_configuration["probes"] = probes_cfg

        probes = build_probes(
            ProbeBuildContext(
                experiment_type=litmus_name,
                namespace=experiment.target_namespace,
                target_label=experiment.target_label,
                target_port=target_port,
                service_protocol=service_protocol,
                target_clusterip=target_clusterip,
                prom_url=vm_url,
                deployment_thresholds=deployment_thresholds,
                experiment_configuration=experiment_configuration,
            ),
            settings=settings
        )

        await chaos_repo.update(
            experiment,
            baseline_metrics=baseline.metrics,
            baseline_start=baseline.start.timestamp(),
            baseline_end=baseline.end.timestamp(),
        )
        await session.commit()

        # FAULT PHASE
        await _report("INJECTING_CHAOS")
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
            msg = "Failed to fetch Chaos Injected Time"
            raise ValueError(msg)

        # RECOVERY PHASE
        await _report("RECOVERY")
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

        await _report("RECORDING_RESULTS")
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
                "Post recovery cluster readiness check failed for experiment=%s: %s",
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
        await _report("CLEANUP")
        try:
            await litmus_manager.delete_experiment(
                kubeconfig_content=kubeconfig,
                engine_name=engine_name,
                namespace=experiment.target_namespace,
            )
        except Exception:
            logger.warning("Failed to cleanup engine %s", engine_name, exc_info=True)


async def _run_chaos_experiment_phases(  # noqa: PLR0913
    job_repo: JobRepository,
    chaos_repo: ChaosRepository,
    job: Job,
    session: Any,  # noqa: ANN401
    cluster: Cluster,
    experiment: ChaosExperiment,
) -> dict[str, str]:
    """Run a single Chaos Experiment: prepare the cluster (operator + RBAC),
    resolve the target service, then delegate to the shared single-run logic
    also used by campaigns."""

    progress = JobProgress(job_repo, job)
    litmus_manager = build_litmus_manager()

    kubeconfig = cluster.kubeconfig
    litmus_name = TYPE_TO_LITMUS_NAME[experiment.experiment_type]
    engine_name = experiment.chaos_engine_name

    if not kubeconfig:
        msg = "kubeconfig is missing"
        raise ValueError(msg)

    if not engine_name:
        msg = "chaos_engine_name is missing"
        raise ValueError(msg)

    # PHASE 1: VALIDATING (5%)
    await job_repo.update(
        job,
        status=JobStatus.RUNNING,
        current_phase="VALIDATING",
        progress_percentage=5,
    )
    await session.commit()

    is_ready = await litmus_manager.verify_operator(kubeconfig)
    if not is_ready:
        msg = "Litmus Operator is not running"
        raise RuntimeError(msg)

    # PHASE 2: PREPARING_RBAC (15%)
    await progress("PREPARING_RBAC", 15)
    await litmus_manager.setup_experiment_rbac(
        kubeconfig_content=kubeconfig, namespace=experiment.target_namespace
    )

    # PHASE 3: RESOLVING_TARGET (25%)
    await progress("RESOLVING_TARGET", 25)
    service_target = await resolve_service_target(
        kubeconfig, experiment.target_namespace, experiment.target_label
    )
    target_port = (
        str(service_target["port"])
        if service_target and "port" in service_target
        else str(settings.PROBE_DEFAULT_TARGET_PORT)
    )
    service_protocol = (
        str(service_target.get("protocol", "http")) if service_target else "http"
    )
    target_clusterip = (
        str(service_target.get("clusterIP", "")) if service_target else ""
    )

    # PHASE 4: baseline → fault → recovery (shared single-run logic).
    # Map the shared phase labels onto this job's 30→95 progress band.
    _phase_pct = {
        "BASELINE": 30,
        "INJECTING_CHAOS": 50,
        "RECOVERY": 75,
        "RECORDING_RESULTS": 90,
        "CLEANUP": 95,
    }

    async def on_phase(phase: str) -> None:
        await progress(phase, _phase_pct.get(phase, 50))

    verdict = await _run_single_experiment(
        litmus_manager=litmus_manager,
        chaos_repo=chaos_repo,
        kubeconfig=kubeconfig,
        experiment=experiment,
        litmus_name=litmus_name,
        target_port=target_port,
        service_protocol=service_protocol,
        target_clusterip=target_clusterip,
        session=session,
        vm_url=cluster.victoriametrics_url,
        health_path=None,
        on_phase=on_phase,
    )

    if verdict == "Error":
        # The shared logic already marked the experiment FAILED with a detailed
        # message; re-raise so the outer handler records the job failure too.
        msg = experiment.status_message or "Chaos experiment failed"
        raise RuntimeError(msg)

    # PHASE 5: COMPLETE (100%)
    await job_repo.update(
        job,
        current_phase="COMPLETE",
        progress_percentage=100,
        status=JobStatus.COMPLETED,
        completed_at=datetime.now(UTC),
    )
    await session.commit()

    return {"experiment_id": str(experiment.id), "verdict": verdict}


async def _run_chaos_experiment(experiment_id: str, job_id: str) -> dict[str, str]:
    eid = uuid.UUID(experiment_id)
    jid = uuid.UUID(job_id)

    async with _make_session_maker()() as session:
        chaos_repo = ChaosRepository(session)
        job_repo = JobRepository(session)
        cluster_repo = ClusterRepository(session)

        experiment = await chaos_repo.get_by_id(eid)
        job = await job_repo.get_by_id(jid)
        if not experiment or not job:
            msg = f"Chaos Experiment {experiment_id} or Job {job_id} not found"
            raise ValueError(msg)
        cluster = await cluster_repo.get_by_id(experiment.cluster_id)
        if not cluster:
            msg = f"Cluster {experiment.cluster_id} not found"
            raise ValueError(msg)

        try:
            result = await _run_chaos_experiment_phases(
                chaos_repo=chaos_repo,
                job_repo=job_repo,
                job=job,
                session=session,
                cluster=cluster,
                experiment=experiment,
            )
        except Exception as exc:
            await session.rollback()
            logger.exception(
                "Running Chaos Experiment failed",
                extra={"experiment_id": experiment_id, "job_id": job_id},
            )
            await _record_chaos_experiment_failure(
                experiment_id=eid, job_id=jid, exc=exc
            )
            raise

        logger.info("Chaos Experiment %s running successfully", experiment_id)
        return result
