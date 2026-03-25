import asyncio
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from app.infrastructure.chaos.manager import LitmusChaosManager
from app.models.chaos import ChaosExperiment, ChaosExperimentStatus, ExperimentType
from app.models.cluster import Cluster
from app.models.job import Job, JobStatus
from app.repositories.chaos import ChaosRepository
from app.repositories.cluster import ClusterRepository
from app.repositories.job import JobRepository
from app.tasks.celery_config import celery_app
from app.tasks.shared import _make_session_maker
from app.utils.config import settings

logger = logging.getLogger(__name__)

_TYPE_TO_LITMUS_NAME: dict[ExperimentType, str] = {
    ExperimentType.POD_DELETE: "pod-delete",
    ExperimentType.POD_CPU_HOG: "pod-cpu-hog",
    ExperimentType.POD_MEMORY_HOG: "pod-memory-hog",
    ExperimentType.POD_NETWORK_LATENCY: "pod-network-latency",
    ExperimentType.POD_NETWORK_LOSS: "pod-network-loss",
}

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
    """Record a failure on Chaos Experiment and Job in a fresh DB session."""
    async with _make_session_maker()() as err_session:
        c_repo = ChaosRepository(err_session)
        j_repo = JobRepository(err_session)
        chaos_experiment = await c_repo.get_by_id(experiment_id)
        job = await j_repo.get_by_id(job_id)
        status_msg = f"{prefix}{exc!s}" if prefix else str(exc)
        if chaos_experiment:
            await c_repo.update(
                chaos_experiment,
                status=ChaosExperimentStatus.FAILED,
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


async def _run_chaos_experiment_phases(  # noqa: PLR0913
    job_repo: JobRepository,
    chaos_repo: ChaosRepository,
    job: Job,
    session: Any,  # noqa: ANN401
    cluster: Cluster,
    experiment: ChaosExperiment,
) -> dict[str, str]:
    """Execute Chaos Experiment phases in order"""
    litmus_manager = LitmusChaosManager(
        kubectl_binary=settings.KUBECTL_BINARY, litmus_version=settings.LITMUS_VERSION
    )

    kubeconfig = cluster.kubeconfig
    litmus_name = _TYPE_TO_LITMUS_NAME[experiment.experiment_type]
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
        await job_repo.update(
            job,
            current_phase="PREPARING_RBAC",
            progress_percentage=15,
        )
        await session.commit()

        await litmus_manager.setup_experiment_rbac(
            kubeconfig_content=kubeconfig, namespace=experiment.target_namespace
        )

        # PHASE 3: CREATING_EXPERIMENT (30%)
        await job_repo.update(
            job,
            current_phase="CREATING_EXPERIMENT",
            progress_percentage=30,
        )
        await session.commit()

        await litmus_manager.create_experiment(
            kubeconfig_content=kubeconfig,
            namespace=experiment.target_namespace,
            engine_name=engine_name,
            experiment_type=litmus_name,
            app_label=experiment.target_label,
            duration=experiment.duration_seconds,
        )

        await chaos_repo.update(
            experiment,
            status=ChaosExperimentStatus.RUNNING,
            started_at=datetime.now(UTC),
        )

        # PHASE 4: INJECTING_CHAOS (50%)
        await job_repo.update(
            job,
            current_phase="INJECTING_CHAOS",
            progress_percentage=50,
        )
        await session.commit()

        timeout = experiment.duration_seconds + 120
        chaos_result = await litmus_manager.poll_experiment_result(
            kubeconfig_content=kubeconfig,
            engine_name=engine_name,
            experiment_type=litmus_name,
            namespace=experiment.target_namespace,
            timeout=timeout,
        )

        # PHASE 5: RECORDING_RESULTS (90%)
        await job_repo.update(
            job,
            current_phase="RECORDING_RESULTS",
            progress_percentage=90,
        )
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
