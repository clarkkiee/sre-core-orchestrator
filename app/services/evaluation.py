"""Evaluation service: fault window resolution and ISO/IEC 25023 indicator computation."""

import logging
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

from app.infrastructure.metrics.client import VictoriaMetricsClient
from app.models.chaos import ChaosExperiment
from app.models.evaluation_indicator import ISOIndicator, MeasurementScope
from app.models.raw_metric_sample import MetricPhase, RawMetricSample

logger = logging.getLogger(__name__)

RECOVERY_WINDOW_SECONDS = 120

# Formula version strings — bump when a formula changes so old rows coexist.
_FV_SYSTEM_AVAILABILITY = "iso25023.rav1g.v1"
_FV_MEAN_DOWN_TIME = "iso25023.rav2g.v1"
_FV_FAILURE_RATE = "iso25023.rma3g.v1"
_FV_MEAN_RECOVERY_TIME = "iso25023.rre1g.v1"


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
        delta = timedelta(seconds=RECOVERY_WINDOW_SECONDS)
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


def compute_mean_down_time(
    samples: list[RawMetricSample],
) -> dict[str, Any]:
    """RAv-2-G: X = total_down_time / N_breakdowns.

    Returns None if there are no failure episodes.
    """
    if not samples:
        return {"value": None, "sample_count": 0, "episode_count": 0, "extra": None}

    step = _infer_step(samples)
    episodes = _detect_episodes(samples)

    if not episodes:
        return {
            "value": 0.0,
            "sample_count": len(samples),
            "episode_count": 0,
            "extra": {"step_seconds": step},
        }

    down_intervals = [
        (ep_end - ep_start).total_seconds() + step
        for ep_start, ep_end in episodes
    ]
    total_down = sum(down_intervals)
    mdt = total_down / len(episodes)

    ep_serialized = [
        [ep_start.isoformat(), ep_end.isoformat()]
        for ep_start, ep_end in episodes
    ]
    return {
        "value": round(mdt, 3),
        "sample_count": len(samples),
        "episode_count": len(episodes),
        "extra": {"step_seconds": step, "down_episodes": ep_serialized},
    }


def compute_failure_rate(
    samples: list[RawMetricSample],
    window_seconds: float,
) -> dict[str, Any]:
    """RMa-3-G: X = N_failures / observation_duration."""
    if not samples or window_seconds == 0:
        return {"value": None, "sample_count": 0, "episode_count": 0, "extra": None}

    episodes = _detect_episodes(samples)
    rate = len(episodes) / window_seconds

    return {
        "value": round(rate, 6),
        "sample_count": len(samples),
        "episode_count": len(episodes),
        "extra": {"window_seconds": window_seconds},
    }


def compute_mean_recovery_time(
    fault_samples: list[RawMetricSample],
    recovery_samples: list[RawMetricSample],
    fault_end: datetime,
) -> dict[str, Any]:
    """RRe-1-G: X = Σ Aᵢ / n  where Aᵢ = time from fault_end to first UP in recovery.

    If service is already UP at fault_end (never went down), MRT = 0.
    If service never recovered within the recovery window, the last sample timestamp
    is used as a conservative upper bound.
    """
    if not fault_samples:
        return {"value": None, "sample_count": 0, "episode_count": 0, "extra": None}

    episodes = _detect_episodes(fault_samples)
    if not episodes:
        return {
            "value": 0.0,
            "sample_count": len(fault_samples),
            "episode_count": 0,
            "extra": {"note": "no_failure_episodes"},
        }

    recovery_sorted = sorted(recovery_samples, key=lambda s: s.timestamp)
    t_recovered: datetime | None = next(
        (s.timestamp for s in recovery_sorted if s.value == 1.0),
        None,
    )

    if t_recovered is None:
        if recovery_sorted:
            t_recovered = recovery_sorted[-1].timestamp
        else:
            t_recovered = fault_end

    mrt = (t_recovered - fault_end).total_seconds()
    mrt = max(mrt, 0.0)

    return {
        "value": round(mrt, 3),
        "sample_count": len(fault_samples) + len(recovery_samples),
        "episode_count": len(episodes),
        "extra": {
            "fault_end": fault_end.isoformat(),
            "t_recovered": t_recovered.isoformat(),
        },
    }


# ---------------------------------------------------------------------------
# Scope partitioning
# ---------------------------------------------------------------------------

def _label_matches(labels: dict[str, str], target_label: str) -> bool:
    """Return True if the labels dict matches the target_label key=value pair."""
    if "=" not in target_label:
        return False
    key, _, value = target_label.partition("=")
    return labels.get(key.strip()) == value.strip()


def partition_by_scope(
    samples: list[RawMetricSample],
    target_label: str,
) -> dict[MeasurementScope, list[RawMetricSample]]:
    target = [s for s in samples if _label_matches(s.labels, target_label)]
    peer = [s for s in samples if not _label_matches(s.labels, target_label)]
    return {
        MeasurementScope.TARGET: target,
        MeasurementScope.PEER: peer,
        MeasurementScope.NAMESPACE_WIDE: samples,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _infer_step(samples: list[RawMetricSample]) -> float:
    """Estimate the step interval in seconds from consecutive timestamps."""
    if len(samples) < 2:
        return 10.0
    deltas = [
        (samples[i + 1].timestamp - samples[i].timestamp).total_seconds()
        for i in range(min(5, len(samples) - 1))
        if (samples[i + 1].timestamp - samples[i].timestamp).total_seconds() > 0
    ]
    return min(deltas) if deltas else 10.0


def window_seconds(start: datetime, end: datetime) -> float:
    return (end - start).total_seconds()


# ---------------------------------------------------------------------------
# Indicator → row builder
# ---------------------------------------------------------------------------

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
    return {
        "evaluation_id": evaluation_id,
        "indicator": indicator,
        "phase": phase,
        "scope": scope,
        "value": result["value"],
        "sample_count": result["sample_count"],
        "episode_count": result["episode_count"],
        "formula_version": formula_version,
        "extra": result.get("extra"),
    }
