"""Celery task: three-phase ISO/IEC 25023 evaluation triggered after experiment completion."""

import asyncio
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from app.infrastructure.metrics.catalog import MetricCatalog
from app.infrastructure.metrics.client_factory import VictoriaMetricsClientFactory
from app.infrastructure.metrics.query_engine import MetricsQueryEngine
from app.models.evaluation_indicator import ISOIndicator, MeasurementScope
from app.models.raw_metric_sample import MetricPhase, RawMetricSample
from app.repositories.chaos import ChaosRepository
from app.repositories.cluster import ClusterRepository
from app.repositories.evaluation import EvaluationRepository
from app.repositories.raw_metric_sample import RawMetricSampleRepository
from app.services.evaluation import (
    _FV_AVAILABILITY_BLACKBOX_HTTP,
    _FV_AVAILABILITY_LINKERD,
    _FV_AVAILABILITY_POD_READY,
    _FV_CPU_UTILIZATION,
    _FV_ERROR_RATE,
    _FV_FAILURE_RATE,
    _FV_MEAN_DOWN_TIME,
    _FV_MEAN_FAULT_NOTIFICATION_TIME,
    _FV_MEAN_RECOVERY_TIME,
    _FV_MEAN_TIME_TO_FAILURE,
    _FV_MEMORY_UTILIZATION,
    _FV_RESPONSE_TIME_P95,
    FaultWindowResolutionError,
    PhaseWindows,
    build_indicator_rows,
    compute_cpu_utilization,
    compute_error_rate,
    compute_failure_rate,
    compute_mean_down_time,
    compute_mean_fault_notification_time,
    compute_mean_recovery_time,
    compute_mean_time_to_failure,
    compute_memory_utilization,
    compute_response_time_p95,
    compute_system_availability,
    partition_by_scope,
    resolve_fault_window,
    window_seconds,
)
from app.tasks.celery_config import celery_app
from app.tasks.shared import task_session

logger = logging.getLogger(__name__)

_AVAILABILITY_SIGNALS: list[tuple[str, str]] = [
    ("probe_success",         _FV_AVAILABILITY_BLACKBOX_HTTP),
    ("linkerd_success_rate",  _FV_AVAILABILITY_LINKERD),
    ("kube_pod_status_ready", _FV_AVAILABILITY_POD_READY),
]

_PERFORMANCE_SIGNAL_NAMES: list[str] = [
    "linkerd_response_latency_p95_ms",
    "linkerd_error_rate",
    "cadvisor_cpu_cores_used",
    "container_memory_working_set_bytes",
]

_PERF_COMPUTE_MAP: dict[str, tuple[ISOIndicator, str, Any]] = {
    "linkerd_response_latency_p95_ms": (ISOIndicator.RESPONSE_TIME_P95, _FV_RESPONSE_TIME_P95, compute_response_time_p95),
    "linkerd_error_rate": (ISOIndicator.ERROR_RATE, _FV_ERROR_RATE, compute_error_rate),
    "cadvisor_cpu_cores_used": (ISOIndicator.CPU_UTILIZATION, _FV_CPU_UTILIZATION, compute_cpu_utilization),
    "container_memory_working_set_bytes": (ISOIndicator.MEMORY_UTILIZATION, _FV_MEMORY_UTILIZATION, compute_memory_utilization)
}

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

        catalog =  MetricCatalog.load_from_dir()
        query_engine = MetricsQueryEngine(
            client_factory=vm_client_factory,
            catalog=catalog,
            repo=sample_repo
        )

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
                baseline_start=datetime.now(UTC),
                baseline_end=datetime.now(UTC),
                fault_start=datetime.now(UTC),
                fault_end=datetime.now(UTC),
                recovery_start=datetime.now(UTC),
                recovery_end=datetime.now(UTC),
            )
            raise

        # Step 2 wait for recovery window
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
        metric_names = [name for name, _ in _AVAILABILITY_SIGNALS] + _PERFORMANCE_SIGNAL_NAMES
        for phase, start, end in [
            (MetricPhase.BASELINE, windows.baseline_start, windows.baseline_end),
            (MetricPhase.FAULT,    windows.fault_start,    windows.fault_end),
            (MetricPhase.RECOVERY, windows.recovery_start, windows.recovery_end),
        ]:
            await query_engine.fetch_phase(
                experiment_id=experiment_id,
                phase=phase,
                namespace=experiment.target_namespace,
                start=start,
                end=end,
                metric_names=metric_names,
                extra_params={"window": "30s"}
            )

        # Step 4 — load samples from DB and compute indicators
        all_samples: dict[str, dict[MetricPhase, list[RawMetricSample]]] = {}
        for metric_name in metric_names:
            all_samples[metric_name] = {
                phase: await sample_repo.list_by_experiment_phase(
                    experiment_id, phase, metric_name=metric_name
                )
                for phase in MetricPhase
            }

        # Step 5 — upsert envelope
        eval_data = {
            "id": uuid.uuid4(),
            "experiment_id": experiment_id,
            "baseline_start": windows.baseline_start,
            "baseline_end": windows.baseline_end,
            "fault_start": windows.fault_start,
            "fault_end": windows.fault_end,
            "recovery_start": windows.recovery_start,
            "recovery_end": windows.recovery_end,
            "litmus_probe_percentage": _extract_litmus_probe_pct(experiment),
            "status": "SUCCESS",
            "evaluator_version": "v1",
        }
        evaluation = await eval_repo.upsert_evaluation(eval_data)

        # Step 6 — compute and upsert indicator rows
        indicator_rows: list[dict[str, Any]] = []

        for metric_name, fv_avail in _AVAILABILITY_SIGNALS:
            definition = catalog.get(metric_name)
            _compute_availability_for_signal(
                evaluation_id=evaluation.id,
                samples_by_phase=all_samples[metric_name],
                target_label=experiment.target_label,
                scope_label_key=definition.scope_label_key,
                windows=windows,
                formula_version_avail=fv_avail,
                rows=indicator_rows
            )

        probe_scopes_by_phase = {
            phase: partition_by_scope(
                all_samples["probe_success"][phase],
                experiment.target_label,
                scope_label_key=catalog.get("probe_success").scope_label_key,
            )
            for phase in MetricPhase
        }

        _compute_per_phase_indicators(
            evaluation_id=evaluation.id,
            scopes_by_phase=probe_scopes_by_phase,
            windows=windows,
            rows=indicator_rows,
        )
        _compute_mrt(
            evaluation_id=evaluation.id,
            fault_samples=probe_scopes_by_phase[MetricPhase.FAULT],
            recovery_samples=probe_scopes_by_phase[MetricPhase.RECOVERY],
            fault_end=windows.fault_end,
            rows=indicator_rows,
        )

        _compute_mttf(
            evaluation_id=evaluation.id,
            scopes_by_phase=probe_scopes_by_phase,
            windows=windows,
            rows=indicator_rows,
        )

        _compute_mfnt(
            evaluation_id=evaluation.id,
            fault_scopes=probe_scopes_by_phase[MetricPhase.FAULT],
            chaos_injected_time=windows.fault_start,
            rows=indicator_rows
        )

        for signal_name in _PERFORMANCE_SIGNAL_NAMES:
            definition = catalog.get(signal_name)
            _compute_performance_indicators(
                evaluation_id=evaluation.id,
                signal_name=signal_name,
                samples_by_phase=all_samples[signal_name],
                target_label=experiment.target_label,
                scope_label_key=definition.scope_label_key,
                rows=indicator_rows
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

def _compute_mttf(
    evaluation_id: uuid.UUID,
    scopes_by_phase: dict[MetricPhase, dict[MeasurementScope, list[RawMetricSample]]],
    windows: PhaseWindows,
    rows: list[dict[str, Any]],
) -> None:
    for phase, phase_start in [
        (MetricPhase.BASELINE, windows.baseline_start),
        (MetricPhase.FAULT, windows.fault_start)
    ]:
        for scope, samples in scopes_by_phase.get(phase, {}).items():
            result = compute_mean_time_to_failure(samples, phase_start)
            row = build_indicator_rows(
                evaluation_id, ISOIndicator.MEAN_TIME_TO_FAILURE,
                phase, scope, result, _FV_MEAN_TIME_TO_FAILURE,
            )
            if row:
                rows.append(row)

def _compute_mfnt(
    evaluation_id: uuid.UUID,
    fault_scopes: dict[MeasurementScope, list[RawMetricSample]],
    chaos_injected_time: datetime,
    rows: list[dict[str, Any]],
) -> None:
    for scope, samples in fault_scopes.items():
        result = compute_mean_fault_notification_time(samples, chaos_injected_time)
        row = build_indicator_rows(
            evaluation_id, ISOIndicator.MEAN_FAULT_NOTIFICATION_TIME,
            MetricPhase.FAULT, scope, result, _FV_MEAN_FAULT_NOTIFICATION_TIME
        )
        if row:
            rows.append(row)

def _extract_litmus_probe_pct(experiment: Any) -> float | None: # noqa: ANN401
    result = (experiment.result or {})

    pct_str = (
        result.get("status", {})
            .get("experimentStatus", {})
            .get("probeSuccessPercentage")
    )

    try:
        return float(pct_str) if pct_str is not None else None
    except (TypeError, ValueError):
        return None

def _compute_availability_for_signal(
    evaluation_id: uuid.UUID,
    samples_by_phase: dict[MetricPhase, list[RawMetricSample]],
    target_label: str,
    scope_label_key: str | None,
    windows: PhaseWindows,
    formula_version_avail: str,
    rows: list[dict[str, Any]],
) -> None:
    phase_window_map = {
        MetricPhase.BASELINE: window_seconds(windows.baseline_start, windows.baseline_end),
        MetricPhase.FAULT: window_seconds(windows.fault_start, windows.fault_end),
        MetricPhase.RECOVERY: window_seconds(windows.recovery_start, windows.recovery_end)
    }

    scope_by_phase = {
        phase: partition_by_scope(samples, target_label, scope_label_key)
        for phase, samples in samples_by_phase.items()
    }

    for phase, scope_map in scope_by_phase.items():
        ws = phase_window_map[phase]
        for scope, samples in scope_map.items():
            result = compute_system_availability(samples, ws)
            row = build_indicator_rows(
                evaluation_id, ISOIndicator.SYSTEM_AVAILABILITY,
                phase, scope, result, formula_version_avail
            )

            if row:
                rows.append(row)

def _compute_performance_indicators(
    evaluation_id: uuid.UUID,
    signal_name: str,
    samples_by_phase: dict[MetricPhase, list[RawMetricSample]],
    target_label: str,
    scope_label_key: str | None,
    rows: list[dict[str, Any]],
) -> None:
    indicator, fv, compute_fn = _PERF_COMPUTE_MAP[signal_name]
    for phase, samples in samples_by_phase.items():
        scope_map = partition_by_scope(samples, target_label, scope_label_key)
        for scope, scoped_samples in scope_map.items():
            result = compute_fn(scoped_samples)
            row = build_indicator_rows(
                evaluation_id, indicator,
                phase, scope, result, fv
            )
            if row:
                rows.append(row)