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
    _FV_CPU_UTILIZATION,
    _FV_ERROR_RATE,
    _FV_ERROR_RATE_BLACKBOX,
    _FV_LATENCY_P95_DEG_LINKERD_SUCCESS,
    _FV_LATENCY_P95_DEGRADATION,
    _FV_LATENCY_P95_DEGRADATION_BLACKBOX,
    _FV_LATENCY_P95_DEGRADATION_BLACKBOX_PROC,
    _FV_LATENCY_P99_DEG_LINKERD_SUCCESS,
    _FV_LATENCY_P99_DEGRADATION,
    _FV_LATENCY_P99_DEGRADATION_BLACKBOX,
    _FV_LATENCY_P99_DEGRADATION_BLACKBOX_PROC,
    _FV_MEAN_RECOVERY_TIME,
    _FV_MEMORY_UTILIZATION,
    _FV_RESPONSE_TIME_P95,
    _FV_RESPONSE_TIME_P95_BLACKBOX,
    _FV_RESPONSE_TIME_P95_BLACKBOX_PROC,
    _FV_RESPONSE_TIME_P95_LINKERD_SUCCESS,
    _FV_RESPONSE_TIME_P99,
    _FV_RESPONSE_TIME_P99_BLACKBOX,
    _FV_RESPONSE_TIME_P99_BLACKBOX_PROC,
    _FV_RESPONSE_TIME_P99_LINKERD_SUCCESS,
    _FV_SUCCESS_RATE_DEGRADATION,
    _FV_SUCCESS_RATE_DEGRADATION_BLACKBOX,
    FaultWindowResolutionError,
    PhaseWindows,
    build_indicator_rows,
    compute_cpu_utilization,
    compute_error_rate,
    compute_error_rate_blackbox_grpc,
    compute_error_rate_blackbox_http,
    compute_latency_p95_degradation,
    compute_latency_p95_degradation_blackbox,
    compute_latency_p99_degradation,
    compute_latency_p99_degradation_blackbox,
    compute_mean_recovery_time,
    compute_memory_utilization,
    compute_response_time_p95,
    compute_response_time_p95_blackbox,
    compute_response_time_p99,
    compute_response_time_p99_blackbox,
    compute_success_rate_degradation,
    compute_success_rate_degradation_blackbox,
    compute_system_availability,
    partition_by_scope,
    resolve_fault_window,
    window_seconds,
)
from app.tasks.celery_config import celery_app
from app.tasks.shared import task_session

logger = logging.getLogger(__name__)

_BINARY_AVAILABILITY_SIGNALS: list[tuple[str, str]] = [
    ("probe_success", _FV_AVAILABILITY_BLACKBOX_TCP),
]

_PERFORMANCE_SIGNAL_NAMES: list[str] = [
    "linkerd_response_latency_p95_ms",
    "linkerd_response_latency_p99_ms",
    "linkerd_error_rate",
    "cadvisor_cpu_cores_used",
]

# Linkerd success-only variants — fetched separately, compute pakai function
# yang sama dengan Linkerd-all tapi emit row dengan formula_version berbeda.
_LINKERD_SUCCESS_LATENCY_SIGNALS: list[str] = [
    "linkerd_response_latency_p95_success_ms",
    "linkerd_response_latency_p99_success_ms",
]

_AUX_SIGNAL_NAMES: list[str] = [
    "linkerd_success_rate"
]

_MEMORY_SIGNAL_NAMES: list[str] = [
    "container_memory_working_set_bytes",
    "kube_pod_container_restarts_total",
]

# Blackbox-sourced signals fetched in parallel for additive evaluation rows.
# Result rows carry the *_BLACKBOX formula_version so they coexist with the
# Linkerd-sourced rows (different formula_version → no unique-constraint clash).
_BLACKBOX_SIGNAL_NAMES: list[str] = [
    "probe_success_htttp",
    "probe_success_grpc",
    "probe_duration_seconds_http",
    "probe_duration_seconds_grpc",
    "probe_http_status_code",
    "probe_grpc_status_code",
    "probe_http_duration_processing",
]

def _resolve_service_protocol(
    discovered_services: list[dict[str, Any]] | None,
    target_label: str,
    default: str = "http",
) -> str:
    """Lookup service protocol from campaign.discovered_services.

    Returns 'grpc', 'http', or 'tcp' (fallback to `default` if not found).
    Reuses the protocol inference dari `_infer_service_protocol` di
    `app.infrastructure.chaos.discovery` (sudah dijalankan saat campaign start
    dan dipersist ke chaos_campaigns.discovered_services JSONB column).
    """
    if not discovered_services:
        return default
    name = (
        target_label.split("=", 1)[1].strip()
        if "=" in target_label
        else target_label
    )
    for svc in discovered_services:
        if svc.get("name") == name:
            return svc.get("protocol") or default
    return default

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
    "linkerd_response_latency_p95_success_ms": (
        ISOIndicator.RESPONSE_TIME_P95,
        _FV_RESPONSE_TIME_P95_LINKERD_SUCCESS,
        compute_response_time_p95,
    ),
    "linkerd_response_latency_p99_success_ms": (
        ISOIndicator.RESPONSE_TIME_P99,
        _FV_RESPONSE_TIME_P99_LINKERD_SUCCESS,
        compute_response_time_p99,
    ),
    "linkerd_error_rate": (
        ISOIndicator.ERROR_RATE, _FV_ERROR_RATE, compute_error_rate,
    ),
    "cadvisor_cpu_cores_used": (
        ISOIndicator.CPU_UTILIZATION, _FV_CPU_UTILIZATION, compute_cpu_utilization,
    ),
}

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
            + _AUX_SIGNAL_NAMES
            + _PERFORMANCE_SIGNAL_NAMES
            + _LINKERD_SUCCESS_LATENCY_SIGNALS
            + _MEMORY_SIGNAL_NAMES
            + _BLACKBOX_SIGNAL_NAMES
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
            "status": _derive_evaluation_status(experiment),
            "evaluator_version": "v2",
        }
        evaluation = await eval_repo.upsert_evaluation(eval_data)

        probe_rows = _extract_probe_results(experiment)
        await _upsert_probe_results(session, evaluation.id, probe_rows)

        # Step 6 compute and upsert indicator rows
        indicator_rows: list[dict[str, Any]] = []

        # Binary availability signals (probe_success) time-based
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
                  
        # MRT from blackbox exporter
        probe_scopes_by_phase = {
            phase: partition_by_scope(
                all_samples["probe_success"][phase],
                experiment.target_label,
                scope_label_key=catalog.get("probe_success").scope_label_key,
            )
            for phase in MetricPhase
        }

        _compute_mrt(
            evaluation_id=evaluation.id,
            probe_fault_scopes=probe_scopes_by_phase[MetricPhase.FAULT],
            probe_recovery_scopes=probe_scopes_by_phase[MetricPhase.RECOVERY],
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

        # Linkerd success-only latency degradation (industry-standard RED).
        # Cross-phase: (P95/99_fault - P95/99_baseline) / P95/99_baseline,
        # dihitung dari histogram yang hanya berisi request classification=success.
        p95_success_def = catalog.get("linkerd_response_latency_p95_success_ms")
        linkerd_p95_success_scopes_by_phase = {
            phase: partition_by_scope(
                all_samples["linkerd_response_latency_p95_success_ms"][phase],
                experiment.target_label,
                scope_label_key=p95_success_def.scope_label_key,
            )
            for phase in MetricPhase
        }
        for scope in (MeasurementScope.TARGET, MeasurementScope.PEER):
            result = compute_latency_p95_degradation(
                baseline_samples=linkerd_p95_success_scopes_by_phase[MetricPhase.BASELINE].get(scope, []),
                fault_samples=linkerd_p95_success_scopes_by_phase[MetricPhase.FAULT].get(scope, []),
            )
            row = build_indicator_rows(
                evaluation.id, ISOIndicator.LATENCY_P95_DEGRADATION,
                None, scope, result, _FV_LATENCY_P95_DEG_LINKERD_SUCCESS,
            )
            if row:
                indicator_rows.append(row)

        p99_success_def = catalog.get("linkerd_response_latency_p99_success_ms")
        linkerd_p99_success_scopes_by_phase = {
            phase: partition_by_scope(
                all_samples["linkerd_response_latency_p99_success_ms"][phase],
                experiment.target_label,
                scope_label_key=p99_success_def.scope_label_key,
            )
            for phase in MetricPhase
        }
        for scope in (MeasurementScope.TARGET, MeasurementScope.PEER):
            result = compute_latency_p99_degradation(
                baseline_samples=linkerd_p99_success_scopes_by_phase[MetricPhase.BASELINE].get(scope, []),
                fault_samples=linkerd_p99_success_scopes_by_phase[MetricPhase.FAULT].get(scope, []),
            )
            row = build_indicator_rows(
                evaluation.id, ISOIndicator.LATENCY_P99_DEGRADATION,
                None, scope, result, _FV_LATENCY_P99_DEG_LINKERD_SUCCESS,
            )
            if row:
                indicator_rows.append(row)

        for signal_name in _PERFORMANCE_SIGNAL_NAMES + _LINKERD_SUCCESS_LATENCY_SIGNALS:
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

        # Additive: blackbox-sourced indicator rows (parallel formula_version).
        # Resolve service protocol dari campaign.discovered_services (dipersist
        # saat discovery oleh campaign task) — bukan dari hardcoded list.
        # Lookup minimal kolom saja untuk hindari eager-load relationship yang
        # bisa konflik dengan session yang sedang dirty.
        discovered_services: list[dict[str, Any]] | None = None
        if experiment.campaign_id is not None:
            from sqlalchemy import select
            from app.models.campaign import ChaosCampaign
            stmt = select(ChaosCampaign.discovered_services).where(
                ChaosCampaign.id == experiment.campaign_id
            )
            result = await session.execute(stmt)
            discovered_services = result.scalar_one_or_none()

        _compute_blackbox_indicators(
            evaluation_id=evaluation.id,
            all_samples=all_samples,
            target_label=experiment.target_label,
            catalog=catalog,
            rows=indicator_rows,
            discovered_services=discovered_services,
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

def _compute_mrt(
    evaluation_id: uuid.UUID,
    probe_fault_scopes: dict[MeasurementScope, list[RawMetricSample]],
    probe_recovery_scopes: dict[MeasurementScope, list[RawMetricSample]],
    rows: list[dict[str, Any]],
    step_seconds: float = 5.0,
) -> None:
    for scope in (MeasurementScope.TARGET, MeasurementScope.PEER):
       probe_fault = probe_fault_scopes.get(scope, [])
       probe_recovery = probe_recovery_scopes.get(scope, [])
       
       if not probe_fault and not probe_recovery:
           continue
       
       mrt = compute_mean_recovery_time(
           fault_samples=probe_fault,
           recovery_samples=probe_recovery,
           step_seconds=step_seconds,
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

def _derive_evaluation_status(experiment: Any) -> str: # noqa: ANN401
    result = experiment.result or {}
    exp_status = result.get("status", {}).get("experimentStatus", {})
    verdict = exp_status.get("verdict", {})
    
    pct_str = exp_status.get("probeSuccessPercentage")
    try:
        psp = float(pct_str) if pct_str is not None else 0.0
    except (TypeError, ValueError):
        psp = 0.0
        
    if verdict in ("Error", "Stopped"):
        return "FAILED"
    if verdict == "Fail":
        return "FAILED" if psp == 0.0 else "PARTIAL"
    if verdict == "Pass":
        if psp >= 100.0:
            return "SUCCESS"
        if psp >= 50.0:
            return "PARTIAL"
        return "FAILED"
    return "PARTIAL" 

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


def _compute_blackbox_indicators(
    evaluation_id: uuid.UUID,
    all_samples: dict[str, dict[MetricPhase, list[RawMetricSample]]],
    target_label: str,
    catalog: MetricCatalog,
    rows: list[dict[str, Any]],
    discovered_services: list[dict[str, Any]] | None = None,
) -> None:
    """Compute blackbox-sourced indicators and append to rows.

    Additive: produces rows with *_BLACKBOX formula_versions parallel to the
    Linkerd-based rows. Dispatches source metric based on service protocol
    (HTTP vs gRPC) resolved from `discovered_services` per campaign.

    Also emits *_BLACKBOX_PROC variants for LATENCY indicators using
    `probe_http_duration_processing` (server-side phase only, isolating
    handler latency from TCP/DNS/teardown noise floor).
    """
    protocol = _resolve_service_protocol(discovered_services, target_label)

    if protocol == "grpc":
        duration_metric = "probe_duration_seconds_grpc"
        status_metric = "probe_grpc_status_code"
        success_metric = "probe_success_grpc"
        error_compute_fn = compute_error_rate_blackbox_grpc
    else:
        duration_metric = "probe_duration_seconds_http"
        status_metric = "probe_http_status_code"
        success_metric = "probe_success_htttp"
        error_compute_fn = compute_error_rate_blackbox_http

    duration_def = catalog.get(duration_metric)
    status_def = catalog.get(status_metric)
    success_def = catalog.get(success_metric)
    proc_def = catalog.get("probe_http_duration_processing")

    duration_scopes_by_phase = {
        phase: partition_by_scope(
            all_samples[duration_metric][phase],
            target_label,
            scope_label_key=duration_def.scope_label_key,
        )
        for phase in MetricPhase
    }
    status_scopes_by_phase = {
        phase: partition_by_scope(
            all_samples[status_metric][phase],
            target_label,
            scope_label_key=status_def.scope_label_key,
        )
        for phase in MetricPhase
    }
    success_scopes_by_phase = {
        phase: partition_by_scope(
            all_samples[success_metric][phase],
            target_label,
            scope_label_key=success_def.scope_label_key,
        )
        for phase in MetricPhase
    }
    # Processing-phase scopes (HTTP probe only — even for gRPC services, the
    # HTTP probe still runs and phase=processing measures time-to-response).
    # Skip for protocol=tcp where probe layer breakdown isn't meaningful.
    proc_scopes_by_phase: dict[MetricPhase, dict[MeasurementScope, list[RawMetricSample]]] | None = None
    if protocol in {"http", "grpc"}:
        proc_scopes_by_phase = {
            phase: partition_by_scope(
                all_samples["probe_http_duration_processing"][phase],
                target_label,
                scope_label_key=proc_def.scope_label_key,
            )
            for phase in MetricPhase
        }

    # Phase-wise indicators: RESPONSE_TIME_P95/P99 (total + processing) and ERROR_RATE
    for phase in MetricPhase:
        for scope in (MeasurementScope.TARGET, MeasurementScope.PEER):
            duration_samples = duration_scopes_by_phase[phase].get(scope, [])
            status_samples = status_scopes_by_phase[phase].get(scope, [])

            p95_result = compute_response_time_p95_blackbox(duration_samples)
            row = build_indicator_rows(
                evaluation_id, ISOIndicator.RESPONSE_TIME_P95,
                phase, scope, p95_result, _FV_RESPONSE_TIME_P95_BLACKBOX,
            )
            if row:
                rows.append(row)

            p99_result = compute_response_time_p99_blackbox(duration_samples)
            row = build_indicator_rows(
                evaluation_id, ISOIndicator.RESPONSE_TIME_P99,
                phase, scope, p99_result, _FV_RESPONSE_TIME_P99_BLACKBOX,
            )
            if row:
                rows.append(row)

            er_result = error_compute_fn(status_samples)
            row = build_indicator_rows(
                evaluation_id, ISOIndicator.ERROR_RATE,
                phase, scope, er_result, _FV_ERROR_RATE_BLACKBOX,
            )
            if row:
                rows.append(row)

            # Processing-phase variants (SNR-isolated latency)
            if proc_scopes_by_phase is not None:
                proc_samples = proc_scopes_by_phase[phase].get(scope, [])

                p95_proc_result = compute_response_time_p95_blackbox(proc_samples)
                row = build_indicator_rows(
                    evaluation_id, ISOIndicator.RESPONSE_TIME_P95,
                    phase, scope, p95_proc_result, _FV_RESPONSE_TIME_P95_BLACKBOX_PROC,
                )
                if row:
                    rows.append(row)

                p99_proc_result = compute_response_time_p99_blackbox(proc_samples)
                row = build_indicator_rows(
                    evaluation_id, ISOIndicator.RESPONSE_TIME_P99,
                    phase, scope, p99_proc_result, _FV_RESPONSE_TIME_P99_BLACKBOX_PROC,
                )
                if row:
                    rows.append(row)

    # Cross-phase degradation indicators
    for scope in (MeasurementScope.TARGET, MeasurementScope.PEER):
        sr_baseline = success_scopes_by_phase[MetricPhase.BASELINE].get(scope, [])
        sr_fault = success_scopes_by_phase[MetricPhase.FAULT].get(scope, [])
        sr_result = compute_success_rate_degradation_blackbox(sr_baseline, sr_fault)
        row = build_indicator_rows(
            evaluation_id, ISOIndicator.SUCCESS_RATE_DEGRADATION,
            None, scope, sr_result, _FV_SUCCESS_RATE_DEGRADATION_BLACKBOX,
        )
        if row:
            rows.append(row)

        lat_baseline = duration_scopes_by_phase[MetricPhase.BASELINE].get(scope, [])
        lat_fault = duration_scopes_by_phase[MetricPhase.FAULT].get(scope, [])

        p95_deg_result = compute_latency_p95_degradation_blackbox(lat_baseline, lat_fault)
        row = build_indicator_rows(
            evaluation_id, ISOIndicator.LATENCY_P95_DEGRADATION,
            None, scope, p95_deg_result, _FV_LATENCY_P95_DEGRADATION_BLACKBOX,
        )
        if row:
            rows.append(row)

        p99_deg_result = compute_latency_p99_degradation_blackbox(lat_baseline, lat_fault)
        row = build_indicator_rows(
            evaluation_id, ISOIndicator.LATENCY_P99_DEGRADATION,
            None, scope, p99_deg_result, _FV_LATENCY_P99_DEGRADATION_BLACKBOX,
        )
        if row:
            rows.append(row)

        # Processing-phase degradation (SNR-isolated)
        if proc_scopes_by_phase is not None:
            proc_baseline = proc_scopes_by_phase[MetricPhase.BASELINE].get(scope, [])
            proc_fault = proc_scopes_by_phase[MetricPhase.FAULT].get(scope, [])

            p95_proc_deg = compute_latency_p95_degradation_blackbox(proc_baseline, proc_fault)
            row = build_indicator_rows(
                evaluation_id, ISOIndicator.LATENCY_P95_DEGRADATION,
                None, scope, p95_proc_deg, _FV_LATENCY_P95_DEGRADATION_BLACKBOX_PROC,
            )
            if row:
                rows.append(row)

            p99_proc_deg = compute_latency_p99_degradation_blackbox(proc_baseline, proc_fault)
            row = build_indicator_rows(
                evaluation_id, ISOIndicator.LATENCY_P99_DEGRADATION,
                None, scope, p99_proc_deg, _FV_LATENCY_P99_DEGRADATION_BLACKBOX_PROC,
            )
            if row:
                rows.append(row)
