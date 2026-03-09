import asyncio
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from app.models.job import JobStatus
from app.models.observability import MetricSnapshot, SessionStatus
from app.repositories.job import JobRepository
from app.repositories.observability import ObservabilityRepository
from app.tasks.celery_config import celery_app
from app.tasks.shared import _make_session_maker

logger = logging.getLogger(__name__)


# Celery Task Entrypoint
@celery_app.task(  # type: ignore[misc]
    bind=True,
    name="app.tasks.collect_metrics",
    max_retries=1,
    soft_time_limit=1800,
    time_limit=1860,
)
def collect_metrics_task(
    self: Any,  # noqa: ANN401
    session_id: str,
    job_id: str,
) -> dict[str, str]:
    _ = self
    return asyncio.run(_collect_metrics(session_id, job_id))


# Async Implementation


async def _record_collection_failure(
    session_id: uuid.UUID,
    job_id: uuid.UUID,
    exc: Exception,
) -> None:
    async with _make_session_maker()() as err_session:
        obs_repo = ObservabilityRepository(err_session)
        job_repo = JobRepository(err_session)

        session = await obs_repo.get_session_by_id(session_id)
        job = await job_repo.get_by_id(job_id)

        if session:
            await obs_repo.update_session(
                session,
                status=SessionStatus.FAILED,
                status_message=str(exc)[:500],
                completed_at=datetime.now(UTC),
            )
        if job:
            await job_repo.update(
                job,
                status=JobStatus.FAILED,
                error_message=str(exc)[:1000],
                completed_at=datetime.now(UTC),
            )
        await err_session.commit()


async def _run_collection_phase(
    obs_repo: ObservabilityRepository,
    job_repo: JobRepository,
    obs_session: Any,  # noqa: ANN401
    job: Any,  # noqa: ANN401
    db_session: Any,  # noqa: ANN401
) -> dict[str, str]:
    from app.infrastructure.metrics.client import VictoriaMetricsClient
    from app.infrastructure.metrics.sli_registry import (
        SLI_REGISTRY,
        evaluate_slo,
        render_promql,
    )
    from app.repositories.cluster import ClusterRepository

    cluster_repo = ClusterRepository(db_session)
    cluster = await cluster_repo.get_by_id(obs_session.cluster_id)
    if not cluster or not cluster.victoriametrics_url:
        msg = "Cluster or VictoriaMetrics URL not available"
        raise ValueError(msg)

    vm_client = VictoriaMetricsClient(cluster.victoriametrics_url)

    # Phase 1: HEALTH CHECK
    await job_repo.update(
        job,
        status=JobStatus.RUNNING,
        started_at=datetime.now(UTC),
        current_phase="HEALTH_CHECK",
        progress_percentage=5,
    )

    await obs_repo.update_session(
        obs_session,
        status=SessionStatus.COLLECTING,
        started_at=datetime.now(UTC),
    )

    await db_session.commit()

    healthy, detail = await vm_client.check_health()
    if not healthy:
        msg = f"VictoriaMetrics is not reachable: {detail}"
        raise RuntimeError(msg)

    # Phase 2: COLLECTING
    await job_repo.update(job, current_phase="COLLECTING", progress_percentage=10)
    await db_session.commit()

    duration = obs_session.collection_duration_seconds
    interval = obs_session.collection_interval_seconds
    namespace = obs_session.target_namespace
    total_samples = duration // interval

    for sample_idx in range(total_samples):
        snapshots_batch: list[MetricSnapshot] = []

        for sli in SLI_REGISTRY.values():
            promql = render_promql(sli, namespace)
            value: float | None = None
            slo_met: bool | None = None
            raw_response: dict[str, Any] | None = None

            try:
                response = await vm_client.instant_query(promql)
                raw_response = response
                value = vm_client.extract_scalar(response)
                slo_met = evaluate_slo(value, sli)
            except Exception as query_exc:
                logger.warning(
                    "SLI %s query failed: %s",
                    sli.id,
                    query_exc,
                )

            snapshot = MetricSnapshot(
                session_id=obs_session.id,
                sli_name=sli.id,
                sub_characteristics=sli.sub_characteristics,
                display_name=sli.name,
                promql_query=promql,
                value=value,
                unit=sli.unit,
                slo_target=sli.slo_target,
                slo_operator=sli.slo_operator,
                slo_met=slo_met,
                collected_at=datetime.now(UTC),
                raw_response=raw_response,
            )

            snapshots_batch.append(snapshot)

        await obs_repo.create_snapshots_bulk(snapshots=snapshots_batch)

        progress = 10 + int((sample_idx + 1) / total_samples * 80)
        await job_repo.update(job, progress_percentage=progress)
        await db_session.commit()

        # wait for next interval
        if sample_idx < total_samples - 1:
            await asyncio.sleep(interval)

    # Phase 3: COMPLETE
    await obs_repo.update_session(
        obs_session, status=SessionStatus.COMPLETED, completed_at=datetime.now(UTC)
    )

    await job_repo.update(
        job,
        status=JobStatus.COMPLETED,
        current_phase="COMPLETE",
        progress_percentage=100,
        completed_at=datetime.now(UTC),
        result={
            "total_snapshots": total_samples * len(SLI_REGISTRY),
            "samples_collected": total_samples,
            "slis_per_sample": len(SLI_REGISTRY),
        },
    )

    await db_session.commit()

    return {"status": "completed", "session_id": str(obs_session.id)}


async def _collect_metrics(
    session_id: str,
    job_id: str,
) -> dict[str, str]:
    sid = uuid.UUID(session_id)
    jid = uuid.UUID(job_id)

    async with _make_session_maker()() as db_session:
        obs_repo = ObservabilityRepository(db_session)
        job_repo = JobRepository(db_session)

        obs_session = await obs_repo.get_session_by_id(sid)
        job = await job_repo.get_by_id(jid)
        if not obs_session or not job:
            msg = f"Session {session_id} or Job {job_id} not found"
            raise ValueError(msg)

        try:
            result = await _run_collection_phase(
                obs_repo, job_repo, obs_session, job, db_session
            )
        except Exception as exc:
            await db_session.rollback()
            logger.exception(
                "Metrics Collection Failed",
                extra={"session_id": session_id, "job_id": job_id},
            )

            await _record_collection_failure(sid, jid, exc)
            raise

    logger.info("Metrics collection %s completed successfully", session_id)
    return result
