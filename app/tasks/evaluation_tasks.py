"""Celery task: three-phase ISO/IEC 25023 evaluation triggered after experiment completion."""

import asyncio
import logging
import re
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.infrastructure.metrics.catalog import MetricCatalog
from app.infrastructure.metrics.client_factory import VictoriaMetricsClientFactory
from app.infrastructure.metrics.query_engine import MetricsQueryEngine
from app.models.chaos import ChaosExperiment
from app.models.evaluation_indicator import ISOIndicator, MeasurementScope
from app.models.probe_result import ProbeResult
from app.models.raw_metric_sample import MetricPhase, RawMetricSample
from app.repositories.chaos import ChaosRepository
from app.repositories.cluster import ClusterRepository
from app.repositories.evaluation import EvaluationRepository
from app.repositories.raw_metric_sample import RawMetricSampleRepository
from app.services.evaluation import (
    _FV_AVAILABILITY_BLACKBOX_TCP,
    _FV_AVAILABILITY_LINKERD_RATIO,
    _FV_AVAILABILITY_POD_READY,
    _FV_CPU_UTILIZATION,
    _FV_ERROR_RATE,
    _FV_FAULT_TOLERANCE_RATIO,
    _FV_LATENCY_P95_DEGRADATION,
    _FV_LATENCY_P99_DEGRADATION,
    _FV_MEAN_DOWN_TIME,
    _FV_MEAN_RECOVERY_TIME,
    _FV_MEMORY_UTILIZATION,
    _FV_RESPONSE_TIME_P95,
    _FV_RESPONSE_TIME_P99,
    _FV_SUCCESS_RATE_DEGRADATION,
    FaultWindowResolutionError,
    PhaseWindows,
    build_indicator_rows,
    compute_availability_ratio,
    compute_cpu_utilization,
    compute_error_rate,
    compute_fault_tolerance_ratio,
    compute_latency_p95_degradation,
    compute_latency_p99_degradation,
    compute_mean_down_time,
    compute_mean_recovery_time,
    compute_memory_utilization,
    compute_response_time_p95,
    compute_response_time_p99,
    compute_success_rate_degradation,
    compute_system_availability,
    partition_by_scope,
    resolve_fault_window,
    window_seconds,
)
from app.tasks.celery_config import celery_app
from app.tasks.shared import task_session

logger = logging.getLogger(__name__)

_BINARY_AVAILABILITY_SIGNALS: list[tuple[str, str]] = [
    ("probe_success",         _FV_AVAILABILITY_BLACKBOX_TCP),
    ("kube_pod_status_ready", _FV_AVAILABILITY_POD_READY),
]

_RATIO_AVAILABILITY_SIGNALS: list[tuple[str, str]] = [
    ("linkerd_success_rate", _FV_AVAILABILITY_LINKERD_RATIO),
]

_PERFORMANCE_SIGNAL_NAMES: list[str] = [
    "linkerd_response_latency_p95_ms",
    "linkerd_response_latency_p99_ms",
    "linkerd_error_rate",
    "cadvisor_cpu_cores_used",
]

_MEMORY_SIGNAL_NAMES: list[str] = [
    "container_memory_working_set_bytes",
    "kube_pod_container_restarts_total",
]

_PERF_COMPUTE_MAP: dict[str, tuple[ISOIndicator, str, Any]] = {
    "linkerd_response_latency_p95_ms": (
        ISOIndicator.RESPONSE_TIME_P95,
        _FV_RESPONSE_TIME_P95,
        compute_response_time_p95,
    ),
    "linkerd_response_latency_p99_ms": (
        ISOIndicator.RESPONSE_TIME_P99,
        _FV_RESPONSE_TIME_P99,
        compute_response_time_p99,
    ),
    "linkerd_error_rate": (
        ISOIndicator.ERROR_RATE, _FV_ERROR_RATE, compute_error_rate,
    ),
    "cadvisor_cpu_cores_used": (
        ISOIndicator.CPU_UTILIZATION, _FV_CPU_UTILIZATION, compute_cpu_utilization,
    ),
}

# Probe.actual_value is INFORMATIONAL ONLY: extracted for audit/observability so
# operators can compare "what the probe saw" vs the controller-computed indicator.
# It is NOT a source of evaluation_indicators rows — Litmus probes execute
# sequentially at EOT, so each promProbe's range query is anchored to its own
# T_now and drifts from chaos_end. Use the manual collection path for any
# accuracy-critical reporting.
_ACTUAL_VALUE_RE = re.compile(r"Actual value:\s*(.*?)(?:\.\s*Expected value:|$)")

_PROBE_TYPE_MAP: dict[str, str] = {
    "httpProbe": "HTTP",
    "promProbe": "PROM",
    "cmdProbe": "CMD",
    "k8sProbe": "K8S"
}

_PROBE_MODE_MAP: dict[str, str] = {
    "SOT": "SOT",
    "EOT": "EOT",
    "Continuous": "CONTINUOUS",
    "OnChaos": "ON_CHAOS",
    "Edge": "EDGE"
}

_PROBE_VERDICT_MAP: dict[str, str] = {
    "Passed": "PASSED",
    "Failed": "FAILED",
    "NA": "NA",
    "Awaited": "NA"
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

        # Step 3+4 — fetch raw samples for each phase and load from DB
        metric_names = (
            [name for name, _ in _BINARY_AVAILABILITY_SIGNALS]
            + [name for name, _ in _RATIO_AVAILABILITY_SIGNALS]
            + _PERFORMANCE_SIGNAL_NAMES
            + _MEMORY_SIGNAL_NAMES
        )
        all_samples = await _fetch_all_samples(
            query_engine=query_engine,
            sample_repo=sample_repo,
            experiment=experiment,
            windows=windows,
            metric_names=metric_names,
        )

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

        probe_rows = _extract_probe_results(experiment)
        await _upsert_probe_results(session, evaluation.id, probe_rows)

        # Step 6 — compute and upsert indicator rows
        indicator_rows: list[dict[str, Any]] = []

        # Binary availability signals (probe_success, kube_pod_status_ready) — time-based
        for metric_name, fv_avail in _BINARY_AVAILABILITY_SIGNALS:
            definition = catalog.get(metric_name)
            _compute_availability_for_signal(
                evaluation_id=evaluation.id,
                samples_by_phase=all_samples[metric_name],
                target_label=experiment.target_label,
                scope_label_key=definition.scope_label_key,
                windows=windows,
                formula_version_avail=fv_avail,
                rows=indicator_rows,
            )

        # Ratio availability signal (linkerd_success_rate) — request-based
        linkerd_def = catalog.get("linkerd_success_rate")
        linkerd_scopes_by_phase = {
            phase: partition_by_scope(
                all_samples["linkerd_success_rate"][phase],
                experiment.target_label,
                scope_label_key=linkerd_def.scope_label_key,
            )
            for phase in MetricPhase
        }
        for metric_name, fv_avail in _RATIO_AVAILABILITY_SIGNALS:
            _compute_ratio_availability_for_signal(
                evaluation_id=evaluation.id,
                samples_by_phase=all_samples[metric_name],
                target_label=experiment.target_label,
                scope_label_key=catalog.get(metric_name).scope_label_key,
                formula_version_avail=fv_avail,
                rows=indicator_rows,
            )

        # MDT from probe_success (binary episodes)
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
            rows=indicator_rows,
        )

        # MRT and FTR from linkerd_success_rate (ratio signal)
        _compute_mrt(
            evaluation_id=evaluation.id,
            fault_scopes=probe_scopes_by_phase[MetricPhase.FAULT],
            recovery_scopes=probe_scopes_by_phase[MetricPhase.RECOVERY],
            baseline_scopes=probe_scopes_by_phase[MetricPhase.BASELINE],
            fault_end=windows.fault_end,
            rows=indicator_rows,
        )

        _compute_success_rate_degradation(
            rows=indicator_rows,
            baseline_scopes=linkerd_scopes_by_phase[MetricPhase.BASELINE],
            evaluation_id=evaluation.id,
            fault_scopes=linkerd_scopes_by_phase[MetricPhase.FAULT]
        )

        p95_def = catalog.get("linkerd_response_latency_p95_ms")
        linkerd_latency_p95_scopes_by_phase = {
            phase: partition_by_scope(
                all_samples["linkerd_response_latency_p95_ms"][phase],
                experiment.target_label,
                scope_label_key=p95_def.scope_label_key
            )
            for phase in MetricPhase
        }

        _compute_latency_p95_degradation(
            evaluation_id=evaluation.id,
            baseline_scopes=linkerd_latency_p95_scopes_by_phase[MetricPhase.BASELINE],
            fault_scopes=linkerd_latency_p95_scopes_by_phase[MetricPhase.FAULT],
            rows=indicator_rows
        )

        p99_def = catalog.get("linkerd_response_latency_p99_ms")
        linkerd_latency_p99_scopes_by_phase = {
            phase: partition_by_scope(
                all_samples["linkerd_response_latency_p99_ms"][phase],
                experiment.target_label,
                scope_label_key=p99_def.scope_label_key
            )
            for phase in MetricPhase
        }

        _compute_latency_p99_degradation(
            evaluation_id=evaluation.id,
            baseline_scopes=linkerd_latency_p99_scopes_by_phase[MetricPhase.BASELINE],
            fault_scopes=linkerd_latency_p99_scopes_by_phase[MetricPhase.FAULT],
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
                rows=indicator_rows,
            )

        ws_definition = catalog.get("container_memory_working_set_bytes")
        _compute_memory_indicators(
            evaluation_id=evaluation.id,
            memory_samples={k: all_samples[k] for k in _MEMORY_SIGNAL_NAMES},
            target_label=experiment.target_label,
            scope_label_key=ws_definition.scope_label_key,
            rows=indicator_rows,
        )

        # Probe-derived indicator path intentionally disabled: probe.actual_value
        # suffers sequential-execution drift (each EOT promProbe queries from its
        # own T_now, not chaos_end). All evaluation_indicators rows come from the
        # controller's anchored manual collection above. Probe verdicts and
        # actual_value are still persisted on probe_results for audit/observability.

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
    rows: list[dict[str, Any]],
) -> None:
    for phase, scope_map in scopes_by_phase.items():
        for scope, samples in scope_map.items():
            mdt = compute_mean_down_time(samples)
            row = build_indicator_rows(
                evaluation_id, ISOIndicator.MEAN_DOWN_TIME,
                phase, scope, mdt, _FV_MEAN_DOWN_TIME,
            )
            if row:
                rows.append(row)

def _compute_mrt(
    evaluation_id: uuid.UUID,
    fault_scopes: dict[MeasurementScope, list[RawMetricSample]],
    recovery_scopes: dict[MeasurementScope, list[RawMetricSample]],
    baseline_scopes: dict[MeasurementScope, list[RawMetricSample]],
    fault_end: datetime,
    rows: list[dict[str, Any]],
    binary_signal: bool = True,
) -> None:
    for scope in (MeasurementScope.TARGET, MeasurementScope.PEER):
        mrt = compute_mean_recovery_time(
            fault_samples=fault_scopes.get(scope, []),
            fault_end=fault_end,
            recovery_samples=recovery_scopes.get(scope, []),
            binary_signal=binary_signal
        )

        row = build_indicator_rows(
            evaluation_id, ISOIndicator.MEAN_RECOVERY_TIME,
            None, scope, mrt, _FV_MEAN_RECOVERY_TIME
        )

        if row:
            rows.append(row)

async def _fetch_all_samples(
    query_engine: MetricsQueryEngine,
    sample_repo: RawMetricSampleRepository,
    experiment: ChaosExperiment,
    windows: PhaseWindows,
    metric_names: list[str],
) -> dict[str, dict[MetricPhase, list[RawMetricSample]]]:
    for phase, start, end in [
        (MetricPhase.BASELINE, windows.baseline_start, windows.baseline_end),
        (MetricPhase.FAULT,    windows.fault_start,    windows.fault_end),
        (MetricPhase.RECOVERY, windows.recovery_start, windows.recovery_end),
    ]:
        await query_engine.fetch_phase(
            experiment_id=experiment.id,
            phase=phase,
            namespace=experiment.target_namespace,
            start=start,
            end=end,
            metric_names=metric_names,
            extra_params={"window": "30s"},
        )
    result: dict[str, dict[MetricPhase, list[RawMetricSample]]] = {}
    for metric_name in metric_names:
        result[metric_name] = {
            phase: await sample_repo.list_by_experiment_phase(
                experiment.id, phase, metric_name=metric_name
            )
            for phase in MetricPhase
        }
    return result

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

def _extract_probe_results(experiment: Any) -> list[dict[str, Any]]: # noqa: ANN401
    probe_status = (
        (experiment.result or {})
        .get("status", {})
        .get("probeStatuses") or []
    )
    
    rows = []
    for ps in probe_status:
        status = ps.get("status", {})
        description = status.get("description", "")
        
        m = _ACTUAL_VALUE_RE.search(description)
        actual_value = m.group(1).strip() if m else None

        probe_name = ps.get("name", "")
        
        # Map probe name → indicator for cross-reference dashboards.
        # Informational only — does NOT drive evaluation_indicators rows.
        linked_indicator = None
        name_lower = probe_name.lower()
        if "latency-p95" in name_lower:
            linked_indicator = ISOIndicator.RESPONSE_TIME_P95
        elif "latency-p99" in name_lower:
            linked_indicator = ISOIndicator.RESPONSE_TIME_P99
        elif "error-rate" in name_lower:
            linked_indicator = ISOIndicator.ERROR_RATE
        elif "availability" in name_lower:
            linked_indicator = ISOIndicator.SYSTEM_AVAILABILITY

        rows.append({
            "probe_name": probe_name,
            "probe_type": _PROBE_TYPE_MAP.get(ps.get("type", ""), "HTTP"),
            "probe_mode": _PROBE_MODE_MAP.get(ps.get("mode", ""), "SOT"),
            "verdict": _PROBE_VERDICT_MAP.get(status.get("verdict", "NA"), "NA"),
            "actual_value": actual_value,
            "linked_indicator": linked_indicator,
            "description": description
        })

    return rows
    
async def _upsert_probe_results(
    session: Any, # noqa: ANN401
    evaluation_id: uuid.UUID,
    probe_rows: list[dict[str, Any]],
) -> None:
    if not probe_rows:
        return
    
    for row in probe_rows:
        stmt = (
            pg_insert(ProbeResult)
            .values(
                id=uuid.uuid4(),
                evaluation_id=evaluation_id,
                probe_name=row["probe_name"],
                probe_type=row["probe_type"],
                probe_mode=row["probe_mode"],
                verdict=row["verdict"],
                actual_value=row.get("actual_value"),
                linked_indicator=row.get("linked_indicator"),
                attempts=1,
                spec={},
                extra={
                    "description": row.get("description", "")
                }
            )
        ).on_conflict_do_update(
            constraint="uq_probe_results_evaluation_probe",
            set_={
                "verdict": row["verdict"],
                "actual_value": row.get("actual_value"),
                "linked_indicator": row.get("linked_indicator"),
                "extra": {
                    "description": row.get("description", "")
                }
            }
        )
        await session.execute(stmt)
        
    await session.flush()
    
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

def _compute_memory_indicators(
    evaluation_id: uuid.UUID,
    memory_samples: dict[str, dict[MetricPhase, list[RawMetricSample]]],
    target_label: str,
    scope_label_key: str | None,
    rows: list[dict[str, Any]],
) -> None:
    working_set_by_phase = memory_samples["container_memory_working_set_bytes"]
    restarts_by_phase = memory_samples.get("kube_pod_container_restarts_total", {})
    for phase, samples in working_set_by_phase.items():
        scope_map = partition_by_scope(samples, target_label, scope_label_key)
        restart_scope_map = partition_by_scope(
            restarts_by_phase.get(phase, []), target_label, scope_label_key="pod"
        )
        for scope, scoped_samples in scope_map.items():
            restart_samples = restart_scope_map.get(scope, [])
            result = compute_memory_utilization(scoped_samples, restart_samples)
            row = build_indicator_rows(
                evaluation_id, ISOIndicator.MEMORY_UTILIZATION,
                phase, scope, result, _FV_MEMORY_UTILIZATION,
            )
            if row:
                rows.append(row)

def _compute_fault_tolerance_ratio(
    evaluation_id: uuid.UUID,
    baseline_scopes: dict[MeasurementScope, list[RawMetricSample]],
    fault_scopes: dict[MeasurementScope, list[RawMetricSample]],
    rows: list[dict[str,Any]]
) -> None:
    for scope in (MeasurementScope.TARGET, MeasurementScope.PEER):
        result = compute_fault_tolerance_ratio(
            baseline_samples=baseline_scopes.get(scope, []),
            fault_samples=fault_scopes.get(scope, []),
        )
        row = build_indicator_rows(
            evaluation_id, ISOIndicator.FAULT_TOLERANCE_RATIO,
            None, scope, result, _FV_FAULT_TOLERANCE_RATIO,
        )

        if row:
            rows.append(row)

def _compute_ratio_availability_for_signal(
    evaluation_id: uuid.UUID,
    samples_by_phase: dict[MetricPhase, list[RawMetricSample]],
    target_label: str,
    scope_label_key: str | None,
    formula_version_avail: str,
    rows: list[dict[str, Any]]
) -> None:
    for phase, samples in samples_by_phase.items():
        scope_map = partition_by_scope(
            samples, target_label, scope_label_key
        )

        for scope, scoped_samples in scope_map.items():
            result = compute_availability_ratio(scoped_samples)
            row = build_indicator_rows(
                evaluation_id, ISOIndicator.SYSTEM_AVAILABILITY,
                phase, scope, result, formula_version_avail
            )

            if row:
                rows.append(row)

def _compute_success_rate_degradation(
    evaluation_id: uuid.UUID,
    baseline_scopes: dict[MeasurementScope, list[RawMetricSample]],
    fault_scopes: dict[MeasurementScope, list[RawMetricSample]],
    rows: list[dict[str, Any]]
) -> None:

    for scope in (MeasurementScope.TARGET, MeasurementScope.PEER):
        result = compute_success_rate_degradation(
            baseline_samples=baseline_scopes.get(scope, []),
            fault_samples=fault_scopes.get(scope, []),
        )

        row = build_indicator_rows(
            evaluation_id, ISOIndicator.SUCCESS_RATE_DEGRADATION,
            None, scope, result, _FV_SUCCESS_RATE_DEGRADATION
        )

        if row:
            rows.append(row)

def _compute_latency_p95_degradation(
    evaluation_id: uuid.UUID,
    baseline_scopes: dict[MeasurementScope, list[RawMetricSample]],
    fault_scopes: dict[MeasurementScope, list[RawMetricSample]],
    rows: list[dict[str, Any]]
) -> None:

    for scope in (MeasurementScope.TARGET, MeasurementScope.PEER):
        result = compute_latency_p95_degradation(
            baseline_samples=baseline_scopes.get(scope, []),
            fault_samples=fault_scopes.get(scope, []),
        )

        row = build_indicator_rows(
            evaluation_id, ISOIndicator.LATENCY_P95_DEGRADATION,
            None, scope, result, _FV_LATENCY_P95_DEGRADATION
        )

        if row:
            rows.append(row)

def _compute_latency_p99_degradation(
    evaluation_id: uuid.UUID,
    baseline_scopes: dict[MeasurementScope, list[RawMetricSample]],
    fault_scopes: dict[MeasurementScope, list[RawMetricSample]],
    rows: list[dict[str, Any]]
) -> None:

    for scope in (MeasurementScope.TARGET, MeasurementScope.PEER):
        result = compute_latency_p99_degradation(
            baseline_samples=baseline_scopes.get(scope, []),
            fault_samples=fault_scopes.get(scope, []),
        )

        row = build_indicator_rows(
            evaluation_id, ISOIndicator.LATENCY_P99_DEGRADATION,
            None, scope, result, _FV_LATENCY_P99_DEGRADATION
        )

        if row:
            rows.append(row)
