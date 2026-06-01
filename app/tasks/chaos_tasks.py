import asyncio
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.infrastructure.chaos.discovery import (
    resolve_service_target,
)
from app.infrastructure.chaos.naming import TYPE_TO_LITMUS_NAME
from app.infrastructure.chaos.probes import (
    build_probes,
    estimate_eot_probe_overhead_seconds,
)
from app.infrastructure.factories import build_litmus_manager
from app.infrastructure.metrics.client import VictoriaMetricsClient
from app.models.chaos import ChaosExperiment, ChaosExperimentStatus
from app.models.cluster import Cluster
from app.models.job import Job, JobStatus
from app.repositories.chaos import ChaosRepository
from app.repositories.cluster import ClusterRepository
from app.repositories.deployment import DeploymentRepository
from app.repositories.job import JobRepository
from app.tasks.celery_config import celery_app
from app.tasks.shared import JobProgress, _make_session_maker, record_job_failure
from app.utils.config import settings

logger = logging.getLogger(__name__)

_INJECTED_TIME_POLL_TIMEOUT = 60.0
_INJECTED_TIME_POLL_INTERVAL = 5.0

# Runner pod scheduling + container startup + SOT probe execution.
_PRE_CHAOS_OVERHEAD = 35
_POST_CHAOS_OVERHEAD = 60

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


async def _run_chaos_experiment_phases(  # noqa: PLR0913
    job_repo: JobRepository,
    chaos_repo: ChaosRepository,
    job: Job,
    session: Any,  # noqa: ANN401
    cluster: Cluster,
    experiment: ChaosExperiment,
) -> dict[str, str]:
    """Execute Chaos Experiment phases in order"""

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

    try:
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
            raise RuntimeError(msg)  # noqa: TRY301

        # PHASE 2: PREPARING_RBAC (15%)
        await progress("PREPARING_RBAC", 15)

        await litmus_manager.setup_experiment_rbac(
            kubeconfig_content=kubeconfig, namespace=experiment.target_namespace
        )

        # PHASE 3: CREATING_EXPERIMENT (30%)
        await progress("CREATING_EXPERIMENT", 30)

        # Resolve probe templates: 3-layer threshold chain (settings → deployment → experiment).
        deployment_repo = DeploymentRepository(session)
        deployment = await deployment_repo.get_by_id(experiment.deployment_id)
        service_target = await resolve_service_target(
            kubeconfig, experiment.target_namespace, experiment.target_label
        )
        probes = build_probes(
            experiment_type=litmus_name,
            namespace=experiment.target_namespace,
            target_label=experiment.target_label,
            target_port=(
                str(service_target["port"])
                if service_target and "port" in service_target
                else str(settings.PROBE_DEFAULT_TARGET_PORT)
            ),
            service_protocol=(
                str(service_target.get("protocol", "http"))
                if service_target
                else "http"
            ),
            target_clusterip=(
                str(service_target.get("clusterIP", ""))
                if service_target
                else ""
            ),
            prom_url=cluster.victoriametrics_url,
            settings=settings,
            deployment_thresholds=(
                deployment.probe_thresholds if deployment else None
            ),
            experiment_configuration=experiment.configuration,
        )

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

        await chaos_repo.update(
            experiment,
            status=ChaosExperimentStatus.RUNNING,
            started_at=datetime.now(UTC),
        )

        # PHASE 4: INJECTING_CHAOS (50%)
        await progress("INJECTING_CHAOS", 50)

        eot_overhead = estimate_eot_probe_overhead_seconds(probes)
        timeout = experiment.duration_seconds + eot_overhead + _PRE_CHAOS_OVERHEAD + _POST_CHAOS_OVERHEAD
        logger.info(
            "Computed chaos timeout=%ss (duration=%ss, eot_overhead=%ss, pre_chaos_overhead=%ss, post_chaos_overhead=%ss, probes=%d)",
            timeout,
            experiment.duration_seconds,
            eot_overhead,
            _PRE_CHAOS_OVERHEAD,
            _POST_CHAOS_OVERHEAD,
            len(probes),
        )
        chaos_result = await litmus_manager.poll_experiment_result(
            kubeconfig_content=kubeconfig,
            engine_name=engine_name,
            experiment_type=litmus_name,
            namespace=experiment.target_namespace,
            timeout=timeout,
        )

        logger.info(chaos_result)

        # PHASE 5: RECORDING_RESULTS (90%)
        await progress("RECORDING_RESULTS", 90)
        verdict = (
            chaos_result.get("status", {})
            .get("experimentStatus", {})
            .get("verdict", "N/A")
        )

        await chaos_repo.update(
            experiment,
            result=chaos_result,
            status=ChaosExperimentStatus.COMPLETED,
            status_message=f"Verdict: {verdict}",
            completed_at=datetime.now(UTC),
        )
        await session.commit()

        # PHASE 6: CLEANUP (95%)
        await litmus_manager.delete_experiment(
            kubeconfig_content=kubeconfig,
            engine_name=engine_name,
            namespace=experiment.target_namespace,
        )

        # PHASE 7: COMPLETE (100%)
        await job_repo.update(
            job,
            current_phase="COMPLETE",
            progress_percentage=100,
            status=JobStatus.COMPLETED,
            completed_at=datetime.now(UTC),
        )
        await session.commit()

        # Enqueue out-of-band so a metrics backend issue never fails the experiment.
        from app.tasks.evaluation_tasks import evaluate_experiment_task  # noqa: PLC0415
        evaluate_experiment_task.delay(str(experiment.id)) # type: ignore
        logger.info("Enqueued evaluation for experiment=%s", experiment.id)

    except Exception as e:
        logger.exception("Chaos experiment %s failed", experiment.id)
        await chaos_repo.update(
            experiment,
            status=ChaosExperimentStatus.FAILED,
            status_message=str(e),
            completed_at=datetime.now(UTC),
        )
        await job_repo.update(
            job,
            status=JobStatus.FAILED,
            error_message=str(e),
            completed_at=datetime.now(UTC),
        )

        await session.commit()
        raise
    else:
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
