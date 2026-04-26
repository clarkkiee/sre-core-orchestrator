"""Celery task: three-phase ISO/IEC 25023 evaluation triggered after experiment completion."""

import asyncio
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from app.infrastructure.metrics.client_factory import VictoriaMetricsClientFactory
from app.models.evaluation_indicator import ISOIndicator, MeasurementScope
from app.models.raw_metric_sample import MetricPhase, RawMetricSample
from app.repositories.chaos import ChaosRepository
from app.repositories.cluster import ClusterRepository
from app.repositories.evaluation import EvaluationRepository
from app.repositories.raw_metric_sample import RawMetricSampleRepository
from app.services.evaluation import (
    _FV_FAILURE_RATE,
    _FV_MEAN_DOWN_TIME,
    _FV_MEAN_RECOVERY_TIME,
    _FV_SYSTEM_AVAILABILITY,
    FaultWindowResolutionError,
    PhaseWindows,
    build_indicator_rows,
    compute_failure_rate,
    compute_mean_down_time,
    compute_mean_recovery_time,
    compute_system_availability,
    partition_by_scope,
    resolve_fault_window,
    window_seconds,
)
from app.tasks.celery_config import celery_app
from app.tasks.shared import task_session

logger = logging.getLogger(__name__)


@celery_app.task(  # type: ignore[misc]
    bind=True,
    name="app.tasks.evaluate_experiment",
    max_retries=3,
    default_retry_delay=30,
)
def evaluate_experiment_task(self: Any, experiment_id: str) -> dict[str, Any]:  # noqa: ANN401
    _ = self
    return asyncio.run(_evaluate_experiment(uuid.UUID(experiment_id)))


async def _evaluate_experiment(experiment_id: uuid.UUID) -> dict[str, Any]:
    async with task_session() as session:
        chaos_repo = ChaosRepository(session)
        cluster_repo = ClusterRepository(session)
        eval_repo = EvaluationRepository(session)
        sample_repo = RawMetricSampleRepository(session)

        experiment = await chaos_repo.get_by_id(experiment_id)
        if experiment is None:
            msg = f"Experiment {experiment_id} not found"
            raise ValueError(msg)

        cluster = await cluster_repo.get_by_id(experiment.cluster_id)
        if cluster is None or not cluster.victoriametrics_url:
            msg = f"Cluster {experiment.cluster_id} has no VictoriaMetrics URL"
            raise ValueError(msg)

        vm_client_factory = VictoriaMetricsClientFactory(
            cluster_repo=cluster_repo,
            experiment_repo=chaos_repo,
        )
        vm_client = await vm_client_factory.for_experiment(experiment_id)

        # Step 1 — resolve authoritative fault window from chaos-exporter
        try:
            windows = await resolve_fault_window(experiment, vm_client)
        except FaultWindowResolutionError as exc:
            logger.error( # noqa: TRY400
                "Fault window resolution failed for experiment=%s: %s",
                experiment_id, exc,
            )
            await eval_repo.mark_failed(
                experiment_id=experiment_id,
                status_message=str(exc),
                # Provide placeholder windows so NOT NULL columns are satisfied
                baseline_start=datetime.now(UTC),
                baseline_end=datetime.now(UTC),
                fault_start=datetime.now(UTC),
                fault_end=datetime.now(UTC),
                recovery_start=datetime.now(UTC),
                recovery_end=datetime.now(UTC),
            )
            raise

        # Step 2 — guard: wait until recovery window has elapsed
        recovery_done_at = windows.recovery_end
        now = datetime.now(UTC)
        if now < recovery_done_at:
            wait = (recovery_done_at - now).total_seconds()
            logger.info(
                "Waiting %.0fs for recovery window to elapse (experiment=%s)",
                wait, experiment_id,
            )
            await asyncio.sleep(wait)

        # Step 3 — fetch and persist raw samples for each phase
        for phase, start, end in [
            (MetricPhase.BASELINE, windows.baseline_start, windows.baseline_end),
            (MetricPhase.FAULT,    windows.fault_start,    windows.fault_end),
            (MetricPhase.RECOVERY, windows.recovery_start, windows.recovery_end),
        ]:
            await sample_repo.delete_for_experiment_phase(experiment_id, phase)

        await _fetch_phase_samples(
            experiment_id=experiment_id,
            windows=windows,
            vm_client=vm_client,
            sample_repo=sample_repo,
            namespace=experiment.target_namespace,
        )

        # Step 4 — load samples from DB and compute indicators
        baseline_samples = await sample_repo.list_by_experiment_phase(
            experiment_id, MetricPhase.BASELINE, metric_name="probe_success"
        )
        fault_samples = await sample_repo.list_by_experiment_phase(
            experiment_id, MetricPhase.FAULT, metric_name="probe_success"
        )
        recovery_samples = await sample_repo.list_by_experiment_phase(
            experiment_id, MetricPhase.RECOVERY, metric_name="probe_success"
        )

        scopes_by_phase: dict[MetricPhase, dict[MeasurementScope, list[RawMetricSample]]] = {
            MetricPhase.BASELINE: partition_by_scope(baseline_samples, experiment.target_label),
            MetricPhase.FAULT:    partition_by_scope(fault_samples,    experiment.target_label),
            MetricPhase.RECOVERY: partition_by_scope(recovery_samples, experiment.target_label),
        }

        # Step 5 — upsert envelope
        litmus_probe_pct = await _fetch_litmus_probe_percentage(
            experiment.chaos_engine_name or "", vm_client
        )
        eval_data = {
            "id": uuid.uuid4(),
            "experiment_id": experiment_id,
            "baseline_start": windows.baseline_start,
            "baseline_end": windows.baseline_end,
            "fault_start": windows.fault_start,
            "fault_end": windows.fault_end,
            "recovery_start": windows.recovery_start,
            "recovery_end": windows.recovery_end,
            "litmus_probe_percentage": litmus_probe_pct,
            "status": "SUCCESS",
            "evaluator_version": "v1",
        }
        evaluation = await eval_repo.upsert_evaluation(eval_data)

        # Step 6 — compute and upsert indicator rows
        indicator_rows: list[dict[str, Any]] = []

        _compute_per_phase_indicators(
            evaluation_id=evaluation.id,
            scopes_by_phase=scopes_by_phase,
            windows=windows,
            rows=indicator_rows,
        )

        _compute_mrt(
            evaluation_id=evaluation.id,
            fault_samples=scopes_by_phase[MetricPhase.FAULT],
            recovery_samples=scopes_by_phase[MetricPhase.RECOVERY],
            fault_end=windows.fault_end,
            rows=indicator_rows,
        )

        inserted = await eval_repo.upsert_indicators(indicator_rows)

        logger.info(
            "Evaluation complete for experiment=%s: %d indicator rows",
            experiment_id, inserted,
        )
        return {
            "experiment_id": str(experiment_id),
            "evaluation_id": str(evaluation.id),
            "indicator_rows": inserted,
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _fetch_phase_samples(
    experiment_id: uuid.UUID,
    windows: PhaseWindows,
    vm_client: Any,  # noqa: ANN401
    sample_repo: RawMetricSampleRepository,
    namespace: str,
) -> None:
    """Query VM for probe_success across all three phase windows and persist to DB."""
    from app.infrastructure.metrics.query_engine import parse_range_response

    promql = f'probe_success{{job="blackbox-tcp",namespace="{namespace}"}}'

    for phase, start, end in [
        (MetricPhase.BASELINE, windows.baseline_start, windows.baseline_end),
        (MetricPhase.FAULT,    windows.fault_start,    windows.fault_end),
        (MetricPhase.RECOVERY, windows.recovery_start, windows.recovery_end),
    ]:
        resp = await vm_client.range_query(promql=promql, start=start, end=end, step="10s")
        rows = [
            {
                "metric_name": "probe_success",
                "source": "blackbox_exporter",
                "labels": labels,
                "timestamp": ts,
                "value": value,
                "experiment_id": experiment_id,
                "phase": phase,
            }
            for labels, ts, value in parse_range_response(
                resp=resp,
                labels_to_keep=["instance", "job", "namespace", "service"],
            )
        ]
        if rows:
            await sample_repo.bulk_insert(rows)


def _compute_per_phase_indicators(
    evaluation_id: uuid.UUID,
    scopes_by_phase: dict[MetricPhase, dict[MeasurementScope, list[RawMetricSample]]],
    windows: PhaseWindows,
    rows: list[dict[str, Any]],
) -> None:
    phase_window_map = {
        MetricPhase.BASELINE: window_seconds(windows.baseline_start, windows.baseline_end),
        MetricPhase.FAULT:    window_seconds(windows.fault_start,    windows.fault_end),
        MetricPhase.RECOVERY: window_seconds(windows.recovery_start, windows.recovery_end),
    }

    for phase, scope_map in scopes_by_phase.items():
        ws = phase_window_map[phase]

        for scope, samples in scope_map.items():
            avail = compute_system_availability(samples, ws)
            row = build_indicator_rows(
                evaluation_id, ISOIndicator.SYSTEM_AVAILABILITY,
                phase, scope, avail, _FV_SYSTEM_AVAILABILITY,
            )
            if row:
                rows.append(row)

            mdt = compute_mean_down_time(samples)
            row = build_indicator_rows(
                evaluation_id, ISOIndicator.MEAN_DOWN_TIME,
                phase, scope, mdt, _FV_MEAN_DOWN_TIME,
            )
            if row:
                rows.append(row)

            fr = compute_failure_rate(samples, ws)
            row = build_indicator_rows(
                evaluation_id, ISOIndicator.FAILURE_RATE,
                phase, scope, fr, _FV_FAILURE_RATE,
            )
            if row:
                rows.append(row)


def _compute_mrt(
    evaluation_id: uuid.UUID,
    fault_samples: dict[MeasurementScope, list[RawMetricSample]],
    recovery_samples: dict[MeasurementScope, list[RawMetricSample]],
    fault_end: datetime,
    rows: list[dict[str, Any]],
) -> None:
    for scope in MeasurementScope:
        mrt = compute_mean_recovery_time(
            fault_samples=fault_samples.get(scope, []),
            recovery_samples=recovery_samples.get(scope, []),
            fault_end=fault_end,
        )
        row = build_indicator_rows(
            evaluation_id, ISOIndicator.MEAN_RECOVERY_TIME,
            None, scope, mrt, _FV_MEAN_RECOVERY_TIME,
        )
        if row:
            rows.append(row)


async def _fetch_litmus_probe_percentage(
    engine_name: str,
    vm_client: Any,  # noqa: ANN401
) -> float | None:
    if not engine_name:
        return None
    try:
        promql = f'litmuschaos_probe_success_percentage{{chaosengine_name="{engine_name}"}}'
        resp = await vm_client.instant_query(promql)
        return vm_client.extract_scalar(resp)
    except Exception:
        return None
