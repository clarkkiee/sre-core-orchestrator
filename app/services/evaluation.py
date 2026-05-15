"""Evaluation service: fault window resolution and ISO/IEC 25023 indicator computation."""

import logging
import statistics
from datetime import UTC, datetime, timedelta
from typing import Any

from app.infrastructure.metrics.client import VictoriaMetricsClient
from app.models.chaos import ChaosExperiment
from app.infrastructure.metrics.catalog import MetricCatalog
from app.infrastructure.metrics.query_engine import parse_range_response
from app.models.evaluation_indicator import (
    ISOIndicator,
    MeasurementScope,
    derive_sub_characteristic,
)
from app.models.raw_metric_sample import MetricPhase, RawMetricSample

logger = logging.getLogger(__name__)

EVALUATION_WINDOW_SECONDS = 120

# Formula version strings — bump when a formula changes so old rows coexist.
_FV_MEAN_RECOVERY_TIME = "iso25023.rre1g.v3"
_FV_AVAILABILITY_BLACKBOX_TCP = "iso25010.availability.blackbox_tcp.v1"
_FV_RESPONSE_TIME_P95           = "iso25023.ptb2g.p95.v1"
_FV_RESPONSE_TIME_P99           = "iso25023.ptb2g.p99.v1"
_FV_ERROR_RATE                  = "iso25010.error_rate.v1"
_FV_CPU_UTILIZATION             = "iso25023.pru1g.v1"
_FV_MEMORY_UTILIZATION          = "iso25023.pru2g.v1"
_FV_SUCCESS_RATE_DEGRADATION = "iso25010.fault_tolerance.success_rate_ratio.v1"
_FV_LATENCY_P95_DEGRADATION = "iso25010.fault_tolerance.latency_p95_ratio.v1"
_FV_LATENCY_P99_DEGRADATION = "iso25010.fault_tolerance.latency_p99_ratio.v1"

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
        except KeyError as e:
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


# ---------------------------------------------------------------------------
# Indicator computation from raw_metric_samples
# ---------------------------------------------------------------------------

def _detect_episodes(
    samples: list[RawMetricSample],
) -> list[tuple[datetime, datetime]]:
    """Return list of (episode_start, episode_end) for contiguous DOWN runs."""
    episodes: list[tuple[datetime, datetime]] = []
    ep_start: datetime | None = None

    for s in samples:
        is_down = s.value == 0.0
        if is_down and ep_start is None:
            ep_start = s.timestamp
        elif not is_down and ep_start is not None:
            episodes.append((ep_start, s.timestamp))
            ep_start = None

    if ep_start is not None and samples:
        episodes.append((ep_start, samples[-1].timestamp))

    return episodes


def compute_system_availability(
    samples: list[RawMetricSample],
    window_seconds: float,
) -> dict[str, Any]:
    """RAv-1-G: A = uptime_provided / scheduled_uptime.

    uptime_provided  = Σ(probe_success=1) x step_seconds
    scheduled_uptime = window_seconds
    """
    if not samples or window_seconds == 0:
        return {"value": None, "sample_count": 0, "episode_count": 0, "extra": None}

    step = _infer_step(samples)
    up_time = sum(s.value for s in samples) * step
    availability = up_time / window_seconds

    episodes = _detect_episodes(samples)
    return {
        "value": round(min(availability, 1.0), 6),
        "sample_count": len(samples),
        "episode_count": len(episodes),
        "extra": {"step_seconds": step},
    }

def compute_mean_recovery_time(
    fault_samples: list[RawMetricSample],
    recovery_samples: list[RawMetricSample],
    step_seconds: float = 5.0
) -> dict[str, Any]:
    
    all_samples = sorted(
        [s for s in (fault_samples + recovery_samples) if s.value is not None],
        key=lambda s: s.timestamp
    )
    
    if not all_samples:
        return {
            "value": None,
            "sample_count": 0,
            "episode_count": 0,
            "extra": None,
        }
        
    episodes: list[tuple[datetime, datetime]] = []
    ep_start: datetime | None = None
    ep_last: datetime | None = None

    for s in all_samples:
        is_down = (s.value == 0.0)
        if is_down:
            if ep_start is None:
                ep_start = s.timestamp
            ep_last = s.timestamp
        else:
            if ep_start is not None and ep_last is not None:
                episodes.append((ep_start, ep_last))
                ep_start = None
                ep_last = None
                
    # jika episode kegagalan masih terbuka hingga akhir window
    incomplete = ep_start is not None and ep_last is not None
    if incomplete:
        episodes.append((ep_start, ep_last))  # type: ignore
        
    if not episodes:
        return {
            "value": 0.0,
            "sample_count": len(all_samples),
            "episode_count": 0,
            "extra": {
                "note": "no_failure_episodes",
                "source": "probe_success"
            }
        }
    
    durations = [
        (end - start).total_seconds() + step_seconds
        for start, end in episodes
    ]
    
    mrt = sum(durations) / len(durations)
    
    extra: dict[str, Any] = {
        "source": "probe_success",
        "episode_durations_s": [round(d, 3) for d in durations],
        "step_seconds": step_seconds,
    }
    
    if incomplete:
        extra["note"] = "incomplete_recovery"
    
    return {
        "value": round(mrt, 3),
        "sample_count": len(all_samples),
        "episode_count": len(episodes),
        "extra": extra
    }

def _detect_episodes_ratio(
    samples: list[RawMetricSample],
    threshold: float = 0.95,
) -> list[tuple[datetime, datetime]]:
    episodes: list[tuple[datetime, datetime]] = []
    ep_start: datetime | None = None

    for s in samples:
        is_degraded = s.value is not None and s.value < threshold

        if is_degraded and ep_start is None:
            ep_start = s.timestamp
        elif not is_degraded and ep_start is not None:
            episodes.append((ep_start, s.timestamp))
            ep_start = None

    if ep_start is not None and samples:
        episodes.append((ep_start, samples[-1].timestamp))

    return episodes

def _compute_response_latency(
    samples: list[RawMetricSample],
) -> dict[str, Any]:
    """Shared core for percentile latency indicators (p95/p99).

    Caller is responsible for sourcing the right histogram_quantile samples
    (e.g. linkerd_response_latency_p95_ms vs _p99_ms) and tagging the resulting
    indicator/formula_version accordingly.
    """
    if not samples:
        return {"value": None, "sample_count": 0, "episode_count": 0, "extra": None}

    values = [s.value for s in samples if s.value is not None]
    if not values:
        return {"value": None, "sample_count": len(samples), "episode_count": 0, "extra": None}

    mean_lat = sum(values) / len(values)
    peak_lat = max(values)

    return {
        "value": round(mean_lat, 3),
        "sample_count": len(samples),
        "episode_count": 0,
        "extra": {
            "mean_ms": round(mean_lat, 3),
            "peak_ms": round(peak_lat, 3),
        },
    }


def compute_response_time_p95(
    samples: list[RawMetricSample]
) -> dict[str, Any]:
    return _compute_response_latency(samples)

def compute_response_time_p99(
    samples: list[RawMetricSample]
) -> dict[str, Any]:
    return _compute_response_latency(samples)

def compute_error_rate(
    samples: list[RawMetricSample]
) -> dict[str, Any]:
    """Error rate: rerata rasio error dalam time window tertentu"""
    if not samples:
        return {"value": None, "sample_count": 0, "episode_count": 0, "extra": None}

    values = [s.value for s in samples if s.value is not None]
    if not values:
        return {
            "value": None,
            "sample_count": len(samples),
            "episode_count": 0,
            "extra": None
        }

    mean_rate = sum(values) / len(values)
    peak_rate = max(values)

    return {
        "value": round(mean_rate, 6),
        "sample_count": len(samples),
        "episode_count": 0,
        "extra": {
            "peak_error_rate": round(peak_rate, 6)
        }
    }

def compute_cpu_utilization(
    samples: list[RawMetricSample]
) -> dict[str, Any]:
    """mean and peak penggunaan CPU dalam time window tertentu
    samples berisi rate(container_cpu_usage_seconds_total[$window])_
    """

    if not samples:
        return {"value": None, "sample_count": 0, "episode_count": 0, "extra": None}

    values = [s.value for s in samples if s.value is not None and s.value >= 0]
    if not values:
        return {"value": None, "sample_count": len(samples), "episode_count": 0, "extra": None}

    mean_cores = sum(values) / len(values)
    peak_cores = max(values)

    return {
        "value": round(mean_cores, 6),
        "sample_count": len(samples),
        "episode_count": 0,
        "extra": {
            "mean_cores": round(mean_cores, 6),
            "peak_cores": round(peak_cores, 6)
        }
    }

def compute_memory_utilization(
    samples: list[RawMetricSample],
    restart_samples: list[RawMetricSample] | None = None
) -> dict[str, Any]:
    """mean and peak penggunaan memory dalam time window tertentu
    samples berisi container_memory_working_set_bytes_
    """

    if not samples:
        return {"value": None, "sample_count": 0, "episode_count": 0, "extra": None}

    values = [s.value for s in samples if s.value is not None and s.value >= 0]
    if not values:
        return {"value": None, "sample_count": len(samples), "episode_count": 0, "extra": None}

    mean_bytes = sum(values) / len(values)
    peak_bytes = max(values)

    restart_count = (
        sum(s.value for s in restart_samples if s.value is not None)
        if restart_samples else 0
    )

    return {
        "value": round(mean_bytes, 2),
        "sample_count": len(samples),
        "episode_count": 0,
        "extra": {
            "mean_bytes": round(mean_bytes, 2),
            "peak_bytes": round(peak_bytes, 2),
            "restart_count": round(restart_count)
        }
    }

def compute_success_rate_degradation(
    baseline_samples: list[RawMetricSample],
    fault_samples: list[RawMetricSample]
) -> dict[str, Any]:
    """(SR_baseline - SR_fault) / SR_baseline dari linkerd_success_rate"""

    baseline_vals = [s.value for s in baseline_samples if s.value is not None]
    fault_vals = [s.value for s in fault_samples if s.value is not None]

    if not baseline_vals or not fault_vals:
        return {
            "value": None,
            "sample_count": 0,
            "episode_count": 0,
            "extra": None
        }

    sr_baseline = sum(baseline_vals) / len(baseline_vals)
    sr_fault = sum(fault_vals) / len(fault_vals)

    if sr_baseline == 0.0:
        return {
            "value": None,
            "sample_count": 0,
            "episode_count": 0,
            "extra": {
                "note": "baseline_success_rate_zero"
            }
        }

    return {
        "value": round((sr_baseline - sr_fault) / sr_baseline, 6),
        "sample_count": len(baseline_samples) + len(fault_samples),
        "episode_count": 0,
        "extra": {
            "sr_baseline": round(sr_baseline, 6),
            "sr_fault": round(sr_fault, 6)
        }
    }

def _compute_latency_degradation(
    baseline_samples: list[RawMetricSample],
    fault_samples: list[RawMetricSample],
    percentile_label: str,
) -> dict[str, Any]:
    """Shared core: (P_fault - P_baseline) / P_baseline. percentile_label labels extra fields."""
    baseline_vals = [s.value for s in baseline_samples if s.value is not None]
    fault_vals = [s.value for s in fault_samples if s.value is not None]

    if not baseline_vals or not fault_vals:
        return {"value": None, "sample_count": 0, "episode_count": 0, "extra": None}

    lat_baseline = sum(baseline_vals) / len(baseline_vals)
    lat_fault = sum(fault_vals) / len(fault_vals)

    if lat_baseline == 0.0:
        return {
            "value": None,
            "sample_count": 0,
            "episode_count": 0,
            "extra": {"note": f"baseline_{percentile_label}_zero"},
        }

    return {
        "value": round((lat_fault - lat_baseline) / lat_baseline, 6),
        "sample_count": len(baseline_samples) + len(fault_samples),
        "episode_count": 0,
        "extra": {
            f"{percentile_label}_baseline": round(lat_baseline, 6),
            f"{percentile_label}_fault": round(lat_fault, 6),
        },
    }


def compute_latency_p95_degradation(
    baseline_samples: list[RawMetricSample],
    fault_samples: list[RawMetricSample],
) -> dict[str, Any]:
    """(P95_fault - P95_baseline) / P95_baseline dari linkerd_response_latency_p95_ms"""
    return _compute_latency_degradation(baseline_samples, fault_samples, "p95")


def compute_latency_p99_degradation(
    baseline_samples: list[RawMetricSample],
    fault_samples: list[RawMetricSample],
) -> dict[str, Any]:
    """(P99_fault - P99_baseline) / P99_baseline dari linkerd_response_latency_p99_ms"""
    return _compute_latency_degradation(baseline_samples, fault_samples, "p99")


# ---------------------------------------------------------------------------
# Scope partitioning
# ---------------------------------------------------------------------------

def _label_matches(labels: dict[str, str], target_label: str) -> bool:
    """Return True if the labels dict matches the target_label key=value pair."""
    if "=" not in target_label:
        return False
    key, _, value = target_label.partition("=")
    return labels.get(key.strip()) == value.strip()

def _matches_scope_key(
    labels: dict[str, str],
    scope_label_key: str,
    target_value: str,
) -> bool:
    label_value = labels.get(scope_label_key, "")
    if not label_value:
        return False
    if scope_label_key == "pod":
        k8s_pod_name_segments = 3
        parts = label_value.rsplit("-", 2)
        if len(parts) >= k8s_pod_name_segments:
            return parts[0] == target_value
        return label_value == target_value
    return label_value == target_value

def partition_by_scope(
    samples: list[RawMetricSample],
    target_label: str,
    scope_label_key: str | None = None
) -> dict[MeasurementScope, list[RawMetricSample]]:
    if scope_label_key:
        _, _, target_value = target_label.partition("=")
        target_value = target_value.strip()
        target = [s for s in samples if _matches_scope_key(s.labels, scope_label_key, target_value)]
    else:
        target = [s for s in samples if _label_matches(s.labels, target_label)]

    target_set = set(id(s) for s in target)
    peer = [s for s in samples if id(s) not in target_set]
    return {
        MeasurementScope.TARGET: target,
        MeasurementScope.PEER: peer,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _infer_step(samples: list[RawMetricSample], default: float = 5.0) -> float:
    """Estimate the step interval in seconds from consecutive timestamps."""
    if len(samples) < 2:
        return default
    
    from collections import defaultdict
    by_series = defaultdict(list)
    
    for s in samples:
        key = frozenset(s.labels.items()) if s.labels else frozenset()
        by_series[key].append(s.timestamp)
        
    series_steps = []
    for ts_list in by_series.values():
        if len(ts_list) < 2:
            continue
        ts_sorted = sorted(ts_list)
        deltas = [(ts_sorted[i+1] - ts_sorted[i]).total_seconds()
                  for i in range(len(ts_sorted) - 1)]
        deltas = [d for d in deltas if d > 0]
        if deltas:
            series_steps.append(statistics.median(deltas))
    return statistics.median(series_steps) if series_steps else default
    

def window_seconds(start: datetime, end: datetime) -> float:
    return (end - start).total_seconds()


def build_indicator_rows(
    evaluation_id: Any,
    indicator: ISOIndicator,
    phase: MetricPhase | None,
    scope: MeasurementScope,
    result: dict[str, Any],
    formula_version: str,
) -> dict[str, Any] | None:
    """Return a dict suitable for inserting into evaluation_indicators, or None if value is None."""
    if result["value"] is None:
        return None

    phase_value = phase.value if phase is not None else None

    return {
        "evaluation_id": evaluation_id,
        "indicator": indicator,
        "sub_characteristic": derive_sub_characteristic(indicator, phase_value), 
        "phase": phase,
        "scope": scope,
        "value": result["value"],
        "sample_count": result["sample_count"],
        "episode_count": result["episode_count"],
        "formula_version": formula_version,
        "extra": result.get("extra"),
    }
