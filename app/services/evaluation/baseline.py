"""Baseline metric extraction + fault-window resolution (VictoriaMetrics I/O)."""

import logging
import statistics
from datetime import UTC, datetime, timedelta

from app.infrastructure.metrics.catalog import MetricCatalog
from app.infrastructure.metrics.client import VictoriaMetricsClient
from app.infrastructure.metrics.query_engine import parse_range_response
from app.models.chaos import ChaosExperiment
from app.services.evaluation.indicators import EVALUATION_WINDOW_SECONDS

logger = logging.getLogger(__name__)


async def extract_baseline_metrics(
    vm_client: VictoriaMetricsClient,
    catalog: MetricCatalog,
    namespace: str,
    target_label: str,
    baseline_start: datetime,
    baseline_end: datetime,
) -> dict[str, float | None]:

    metric_to_key = {
        "linkerd_response_latency_p99_ms": "baseline_p99_ms",
        "linkerd_response_latency_p95_ms": "baseline_p95_ms",
        "linkerd_success_rate": "baseline_success_rate",
        "linkerd_error_rate": "baseline_error_rate",
    }

    target_name = (
        target_label.split("=", 1)[1].strip()
        if "=" in target_label else target_label
    )

    params = {"ns": namespace, "window": "30s"}
    result: dict[str, float | None] = {}

    for metric_name, key in metric_to_key.items():

        try:
            definition = catalog.get(metric_name)
        except KeyError:
            logger.warning("Baseline metric not in catalog")
            result[key] = None
            continue

        promql = definition.render(params)
        try:
            resp = await vm_client.range_query(
                promql=promql,
                start=baseline_start,
                end=baseline_end,
                step=definition.default_step
            )
        except Exception as e:
            logger.warning(
                "Baseline metric query failed for %s: %s",
                metric_name, e
            )
            result[key] = None
            continue

        values: list[float] = []
        for labels, _ts, value in parse_range_response(resp, definition.labels_to_keep):
            if labels.get("workload") and labels["workload"] != target_name:
                continue
            values.append(value)

        result[key] = (sum(values) / len(values)) if values else None

    return result


class FaultWindowResolutionError(Exception):
    pass

class PhaseWindows:
    __slots__ = (
        "baseline_end",
        "baseline_start",
        "fault_end",
        "fault_start",
        "recovery_end",
        "recovery_start",
    )

    def __init__(
        self,
        fault_start: datetime,
        fault_end: datetime,
    ) -> None:
        delta = timedelta(seconds=EVALUATION_WINDOW_SECONDS)
        self.fault_start = fault_start
        self.fault_end = fault_end
        self.baseline_start = fault_start - delta
        self.baseline_end = fault_start
        self.recovery_start = fault_end
        self.recovery_end = fault_end + delta


async def resolve_fault_window(
    experiment: ChaosExperiment,
    vm_client: VictoriaMetricsClient,
) -> PhaseWindows:
    """Resolve the authoritative fault window for an experiment.

    Primary source: experiment.chaos_injected_time — captured during experiment
    execution while the chaos-exporter gauge still pointed at this engine, so
    there is no post-hoc gauge-overwrite race.

    Fallback: query VM with last_over_time[5m] at completed_at, for experiments
    that ran before the eager-capture was deployed.

    fault_start = chaos_injected_time (precise injection moment)
    fault_end   = fault_start + duration_seconds (avoids exporter end_time=0 bug)

    Raises FaultWindowResolutionError if neither source yields a valid timestamp.
    """
    engine_name = experiment.chaos_engine_name
    if not engine_name:
        msg = f"Experiment {experiment.id} has no chaos_engine_name"
        raise FaultWindowResolutionError(msg)

    injected_unix: float | None = None
    source: str = "unknown"

    if experiment.chaos_injected_time and experiment.chaos_injected_time > 0:
        injected_unix = experiment.chaos_injected_time
        source = "db"
    else:
        # Fallback: query VM at completed_at using last_over_time to catch the
        # last scraped value for this engine before the exporter moved on.
        query_end = experiment.completed_at or datetime.now(UTC)
        promql = (
            f'last_over_time('
            f'litmuschaos_experiment_chaos_injected_time'
            f'{{chaosengine_name="{engine_name}"}}[5m])'
        )
        resp = await vm_client.instant_query(promql, at=query_end)
        injected_unix = vm_client.extract_scalar(resp)
        source = "vm_fallback"

    if injected_unix is None or injected_unix == 0:
        msg = (
            f"chaos_injected_time not found for engine={engine_name} "
            f"(experiment={experiment.id}). "
            "Ensure chaos-exporter is deployed and scraping."
        )
        raise FaultWindowResolutionError(msg)

    fault_start = datetime.fromtimestamp(injected_unix, tz=UTC)
    fault_end = fault_start + timedelta(seconds=experiment.duration_seconds)

    logger.info(
        "Resolved fault window for experiment=%s (source=%s): %s → %s",
        experiment.id, source, fault_start, fault_end,
    )
    return PhaseWindows(fault_start=fault_start, fault_end=fault_end)

async def extract_baseline_metrics_blackbox(
    vm_client: VictoriaMetricsClient,
    catalog: MetricCatalog,
    namespace: str,
    target_label: str,
    baseline_start: datetime,
    baseline_end: datetime,
    *,
    service_protocol: str = "http",
) -> dict[str, float | None]:
    """Pull blackbox-sourced baseline values (parallel to extract_baseline_metrics).

    service_protocol: "http" or "grpc" — picks the right probe job.
    Returns keys suffixed with `_blackbox` so they coexist with Linkerd keys
    in the same baseline_metrics JSON column.
    """
    if service_protocol == "grpc":
        duration_metric = "probe_duration_seconds_grpc"
        success_metric = "probe_success_grpc"
        status_metric = "probe_grpc_status_code"
    else:
        duration_metric = "probe_duration_seconds_http"
        success_metric = "probe_success_htttp"
        status_metric = "probe_http_status_code"

    target_name = (
        target_label.split("=", 1)[1].strip()
        if "=" in target_label
        else target_label
    )
    params = {"ns": namespace, "window": "30s"}
    result: dict[str, float | None] = {}

    async def _query_mean(metric_name: str) -> float | None:
        try:
            definition = catalog.get(metric_name)
        except KeyError:
            logger.warning("Baseline blackbox metric not in catalog: %s", metric_name)
            return None
        promql = definition.render(params)
        try:
            resp = await vm_client.range_query(
                promql=promql,
                start=baseline_start,
                end=baseline_end,
                step=definition.default_step,
            )
        except Exception as exc:
            logger.warning("Baseline blackbox query failed for %s: %s", metric_name, exc)
            return None
        values: list[float] = []
        for labels, _ts, value in parse_range_response(resp, definition.labels_to_keep):
            if labels.get("service") and labels["service"] != target_name:
                continue
            values.append(value)
        return (sum(values) / len(values)) if values else None

    async def _query_percentile(metric_name: str, percentile: float) -> float | None:
        try:
            definition = catalog.get(metric_name)
        except KeyError:
            return None
        promql = definition.render(params)
        try:
            resp = await vm_client.range_query(
                promql=promql,
                start=baseline_start,
                end=baseline_end,
                step=definition.default_step,
            )
        except Exception as exc:
            logger.warning("Baseline blackbox query failed for %s: %s", metric_name, exc)
            return None
        values: list[float] = []
        for labels, _ts, value in parse_range_response(resp, definition.labels_to_keep):
            if labels.get("service") and labels["service"] != target_name:
                continue
            values.append(value)
        if not values:
            return None
        if len(values) == 1:
            return values[0]
        idx = max(0, min(98, int(round(percentile * 100)) - 1))
        return statistics.quantiles(values, n=100, method="inclusive")[idx]

    result["baseline_success_rate_blackbox"] = await _query_mean(success_metric)
    result["baseline_error_rate_blackbox"] = await _query_mean(status_metric)
    result["baseline_p95_seconds_blackbox"] = await _query_percentile(duration_metric, 0.95)
    result["baseline_p99_seconds_blackbox"] = await _query_percentile(duration_metric, 0.99)

    return result

