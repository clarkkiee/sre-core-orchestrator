"""Tests for `_infer_step` and `compute_system_availability` after BUG-2 fix.

Regression tests for the per-timeseries step inference behavior. Pre-fix,
`_infer_step` computed median delta across aggregate timestamps — when many
timeseries shared the same tick (PEER scope), deltas collapsed to 0 and the
function fell back to the default (10s). That caused PEER availability to be
clamped at 1.0 because `uptime = count × 10 / 120` always exceeded 1.0.

Post-fix, the function groups samples by label-set and takes median of
per-series medians, returning the true scrape cadence (≈5s).

See `notebooks/TEMUAN_VALIDASI.md` (BUG-2) and `notebooks/RENCANA_REVISI_PLATFORM.md`
(Phase 1) for the full context.
"""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from app.services.evaluation import _infer_step, compute_system_availability


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

T0 = datetime(2026, 5, 14, 9, 34, 21, tzinfo=timezone.utc)


def _sample(
    *,
    timestamp: datetime,
    value: float = 1.0,
    labels: dict[str, str] | None = None,
) -> SimpleNamespace:
    """Build a minimal sample with the attributes _infer_step / compute_* read."""
    return SimpleNamespace(
        timestamp=timestamp,
        value=value,
        labels=labels or {},
    )


def _series(
    *,
    workload: str,
    n: int,
    step_s: float = 5.0,
    start: datetime = T0,
    value: float = 1.0,
    extra_labels: dict[str, str] | None = None,
) -> list[SimpleNamespace]:
    """Build a single timeseries: n samples with `step_s` cadence, all same labels."""
    labels = {"workload": workload}
    if extra_labels:
        labels.update(extra_labels)
    return [
        _sample(
            timestamp=start + timedelta(seconds=i * step_s),
            value=value,
            labels=labels,
        )
        for i in range(n)
    ]


# ===========================================================================
# Unit tests: _infer_step
# ===========================================================================


class TestInferStepEdgeCases:
    """Edge cases that must fall back to the default safely."""

    def test_empty_returns_default(self) -> None:
        assert _infer_step([]) == 5.0

    def test_empty_with_custom_default(self) -> None:
        assert _infer_step([], default=2.5) == 2.5

    def test_single_sample_returns_default(self) -> None:
        s = [_sample(timestamp=T0, labels={"workload": "adservice"})]
        assert _infer_step(s) == 5.0

    def test_all_series_have_one_sample_returns_default(self) -> None:
        """11 services × 1 sample each — no series has ≥2 timestamps to compute a delta."""
        samples = [
            _sample(timestamp=T0, labels={"workload": f"svc-{i}"})
            for i in range(11)
        ]
        assert _infer_step(samples) == 5.0


class TestInferStepSingleSeries:
    """Behavior with a single timeseries (TARGET scope is typically this)."""

    def test_regular_5s_cadence(self) -> None:
        samples = _series(workload="adservice", n=25, step_s=5.0)
        assert _infer_step(samples) == 5.0

    def test_regular_10s_cadence(self) -> None:
        samples = _series(workload="adservice", n=12, step_s=10.0)
        assert _infer_step(samples) == 10.0

    def test_irregular_cadence_uses_median(self) -> None:
        """Deltas: 5, 5, 5, 60, 5 — median = 5 (median is robust to single outlier)."""
        deltas = [5, 5, 5, 60, 5]
        ts = [T0]
        for d in deltas:
            ts.append(ts[-1] + timedelta(seconds=d))
        samples = [
            _sample(timestamp=t, labels={"workload": "adservice"})
            for t in ts
        ]
        assert _infer_step(samples) == 5.0

    def test_zero_delta_samples_filtered(self) -> None:
        """Duplicate timestamps should be filtered before computing median."""
        samples = [
            _sample(timestamp=T0, labels={"workload": "adservice"}),
            _sample(timestamp=T0, labels={"workload": "adservice"}),  # duplicate
            _sample(timestamp=T0 + timedelta(seconds=5), labels={"workload": "adservice"}),
            _sample(timestamp=T0 + timedelta(seconds=10), labels={"workload": "adservice"}),
        ]
        assert _infer_step(samples) == 5.0


class TestInferStepMultipleSeriesRegression:
    """Regression tests for BUG-2: multi-series with overlapping timestamps.

    Pre-fix: aggregate-median produced step=0 (most pairs same tick) → fallback 10s.
    Post-fix: per-series median produces correct 5s.
    """

    def test_peer_scope_11_services_synchronized_ticks(self) -> None:
        """11 services × 25 samples, all ticking at the same instants (5s cadence).

        This is the exact pattern that caused PEER step_seconds=10 in the campaign
        baseline (`1e03a16e-...`). Post-fix the function must return 5.0.
        """
        samples = []
        for i in range(11):
            samples.extend(
                _series(workload=f"svc-{i}", n=25, step_s=5.0, start=T0)
            )
        assert _infer_step(samples) == 5.0
        assert len(samples) == 275  # sanity: matches T2.3 PEER sample_count

    def test_target_and_peer_combined_unchanged(self) -> None:
        """Mixing TARGET (1 series) + PEER (11 series) still yields 5s."""
        target = _series(workload="adservice", n=25, step_s=5.0)
        peer = []
        for i in range(11):
            peer.extend(_series(workload=f"peer-{i}", n=25, step_s=5.0))
        assert _infer_step(target + peer) == 5.0

    def test_series_with_different_cadences_median(self) -> None:
        """Half the series at 5s, half at 10s — median of [5,5,10,10] = 7.5."""
        samples = []
        for i in range(2):
            samples.extend(_series(workload=f"fast-{i}", n=10, step_s=5.0))
        for i in range(2):
            samples.extend(_series(workload=f"slow-{i}", n=10, step_s=10.0))
        assert _infer_step(samples) == 7.5

    def test_series_distinguished_by_full_label_set(self) -> None:
        """Same workload label but different pod label → treated as distinct series."""
        samples = []
        samples.extend(
            _series(
                workload="adservice",
                n=25,
                step_s=5.0,
                extra_labels={"pod": "adservice-abc-1"},
            )
        )
        samples.extend(
            _series(
                workload="adservice",
                n=25,
                step_s=5.0,
                extra_labels={"pod": "adservice-abc-2"},
            )
        )
        assert _infer_step(samples) == 5.0

    def test_empty_labels_treated_as_single_series(self) -> None:
        """Samples with empty labels group together as one series (key=frozenset())."""
        samples = [
            _sample(timestamp=T0 + timedelta(seconds=i * 5), labels={})
            for i in range(10)
        ]
        assert _infer_step(samples) == 5.0


# ===========================================================================
# Integration tests: compute_system_availability
# ===========================================================================


class TestSystemAvailabilityStepRegression:
    """End-to-end regression for BUG-2 via compute_system_availability.

    Pre-fix, PEER would have extra.step_seconds=10 → uptime=275×10=2750 →
    availability=22.9 → clamped 1.0. Post-fix step=5 → uptime=275×5=1375 →
    availability=11.5 → still clamped 1.0 BUT the math now reflects reality and
    any drop in probe=1 count proportionally reduces availability (rather than
    needing 12+ sample drops to overcome the inflated step).
    """

    def test_target_step_seconds_is_5_baseline(self) -> None:
        """TARGET scope, baseline phase, all probes up."""
        samples = _series(workload="adservice", n=25, step_s=5.0, value=1.0)
        result = compute_system_availability(samples, window_seconds=120.0)
        assert result["extra"]["step_seconds"] == 5.0
        assert result["value"] == 1.0
        assert result["sample_count"] == 25
        assert result["episode_count"] == 0

    def test_peer_step_seconds_is_5_not_10(self) -> None:
        """PEER scope, 11 synchronized services — MUST be 5.0, not 10.0.

        This is the canonical BUG-2 regression: pre-fix this would return 10.0.
        """
        samples = []
        for i in range(11):
            samples.extend(_series(workload=f"peer-{i}", n=25, step_s=5.0))
        result = compute_system_availability(samples, window_seconds=120.0)
        assert result["extra"]["step_seconds"] == 5.0
        assert result["sample_count"] == 275

    def test_peer_availability_sensitive_to_small_disruption(self) -> None:
        """Drop 6 sample-points across 1 peer service → expect a *measurable* dip.

        Pre-fix: uptime=(275-6)×10=2690, avail=22.4 → clamped 1.0 (no disruption visible).
        Post-fix: uptime=(275-6)×5=1345, avail=11.2 → still clamped 1.0 (not enough drop).

        Note: even post-fix, PEER availability stays clamped at 1.0 unless very many
        samples drop, because total uptime budget is still N_workload × N_sample × step.
        This test documents *that* behavior — Tier 5 insight requires looking at
        per-workload disruption, not aggregate PEER availability.
        """
        samples = []
        for i in range(11):
            samples.extend(_series(workload=f"peer-{i}", n=25, step_s=5.0, value=1.0))
        # Zero out 6 samples on peer-0
        for s in samples[:6]:
            s.value = 0.0
        result = compute_system_availability(samples, window_seconds=120.0)
        assert result["extra"]["step_seconds"] == 5.0  # step still inferred correctly
        # value clamped to 1.0; this is by design of the aggregate-uptime formula
        assert result["value"] == 1.0

    def test_target_availability_drops_when_probe_zero(self) -> None:
        """TARGET, 5 of 25 samples probe=1 — replicates campaign baseline FOCUS exp.

        Expected: uptime = 5×5 = 25s ; availability = 25/120 = 0.208333 (matches
        stored value for eksperimen `0cb13256-...` POD_DELETE adservice).
        """
        samples = _series(workload="adservice", n=25, step_s=5.0, value=0.0)
        for s in samples[:5]:
            s.value = 1.0
        result = compute_system_availability(samples, window_seconds=120.0)
        assert result["extra"]["step_seconds"] == 5.0
        assert result["value"] == pytest.approx(0.208333, abs=1e-5)
        assert result["sample_count"] == 25

    def test_zero_window_returns_none(self) -> None:
        samples = _series(workload="adservice", n=25, step_s=5.0)
        result = compute_system_availability(samples, window_seconds=0)
        assert result["value"] is None

    def test_empty_samples_returns_none(self) -> None:
        result = compute_system_availability([], window_seconds=120.0)
        assert result["value"] is None
        assert result["sample_count"] == 0


# ===========================================================================
# Property test: step inference invariant
# ===========================================================================


class TestInferStepInvariant:
    """Cadence inference must not depend on number of overlapping series."""

    @pytest.mark.parametrize("n_series", [1, 2, 5, 11, 20])
    def test_scaling_series_count_keeps_step_5s(self, n_series: int) -> None:
        """Adding more synchronized series must not change the inferred step.

        This is the property that pre-fix code violated — more series at the same
        tick → more zero-deltas → step inflation.
        """
        samples = []
        for i in range(n_series):
            samples.extend(_series(workload=f"svc-{i}", n=25, step_s=5.0))
        assert _infer_step(samples) == 5.0

    @pytest.mark.parametrize("step_s", [1.0, 2.0, 5.0, 10.0, 15.0])
    def test_arbitrary_step_inferred_correctly(self, step_s: float) -> None:
        """For any reasonable scrape cadence, _infer_step must return that cadence."""
        samples = []
        for i in range(5):
            samples.extend(_series(workload=f"svc-{i}", n=20, step_s=step_s))
        assert _infer_step(samples) == step_s
