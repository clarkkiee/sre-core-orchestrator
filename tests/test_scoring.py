from types import SimpleNamespace

import pytest

from app.infrastructure.metrics.scoring import ReliabilityScorer


def _snap(
    sli_name: str,
    sub: str,
    value: float | None,
    slo_met: bool | None,
) -> SimpleNamespace:
    """Create a mock snapshot."""
    return SimpleNamespace(
        sli_name=sli_name,
        sub_characteristics=sub,
        value=value,
        slo_met=slo_met,
    )


@pytest.fixture
def scorer() -> ReliabilityScorer:
    return ReliabilityScorer()


class TestSubCharacteristicScore:
    def test_all_met(self, scorer: ReliabilityScorer) -> None:
        snaps = [
            _snap("AV-1", "availability", 0.99, True),
            _snap("AV-1", "availability", 0.995, True),
        ]
        assert scorer.compute_sub_characteristic_score(snaps, "availability") == 1.0

    def test_partial_met(self, scorer: ReliabilityScorer) -> None:
        snaps = [
            _snap("AV-1", "availability", 0.99, True),
            _snap("AV-1", "availability", 0.50, False),
        ]
        assert scorer.compute_sub_characteristic_score(snaps, "availability") == 0.5

    def test_none_met(self, scorer: ReliabilityScorer) -> None:
        snaps = [
            _snap("AV-1", "availability", 0.50, False),
            _snap("AV-1", "availability", 0.40, False),
        ]
        assert scorer.compute_sub_characteristic_score(snaps, "availability") == 0.0

    def test_no_relevant_snapshots(self, scorer: ReliabilityScorer) -> None:
        snaps = [_snap("FT-1", "fault_tolerance", 0.5, True)]
        assert scorer.compute_sub_characteristic_score(snaps, "availability") is None

    def test_null_slo_met_excluded(self, scorer: ReliabilityScorer) -> None:
        snaps = [
            _snap("AV-1", "availability", None, None),
            _snap("AV-1", "availability", 0.99, True),
        ]
        assert scorer.compute_sub_characteristic_score(snaps, "availability") == 1.0


class TestOverallScore:
    def test_equal_weights(self, scorer: ReliabilityScorer) -> None:
        scores = {
            "availability": 1.0,
            "faultlessness": 0.8,
            "fault_tolerance": 0.6,
            "recoverability": 1.0,
        }
        result = scorer.compute_overall_score(scores)
        assert result == pytest.approx(0.85)

    def test_skips_none(self, scorer: ReliabilityScorer) -> None:
        scores = {
            "availability": 1.0,
            "faultlessness": None,
            "fault_tolerance": 0.5,
            "recoverability": None,
        }
        result = scorer.compute_overall_score(scores)
        assert result == pytest.approx(0.75)

    def test_all_none(self, scorer: ReliabilityScorer) -> None:
        scores = {"availability": None, "faultlessness": None}
        assert scorer.compute_overall_score(scores) == 0.0

    def test_custom_weights(self, scorer: ReliabilityScorer) -> None:
        scores = {"availability": 1.0, "faultlessness": 0.0}
        weights = {"availability": 0.8, "faultlessness": 0.2}
        result = scorer.compute_overall_score(scores, weights=weights)
        assert result == pytest.approx(0.8)


class TestCompareSessionsAndSliAverages:
    def test_sli_averages(self, scorer: ReliabilityScorer) -> None:
        snaps = [
            _snap("AV-1", "availability", 0.99, True),
            _snap("AV-1", "availability", 0.97, True),
            _snap("FT-1", "fault_tolerance", 0.5, True),
        ]
        avgs = scorer.compute_sli_averages(snaps)
        assert avgs["AV-1"] == pytest.approx(0.98)
        assert avgs["FT-1"] == pytest.approx(0.5)

    def test_compare_sessions(self, scorer: ReliabilityScorer) -> None:
        baseline = [_snap("AV-1", "availability", 0.99, True)]
        chaos = [_snap("AV-1", "availability", 0.50, False)]
        result = scorer.compare_sessions(baseline, chaos)
        assert result["per_sli"]["AV-1"]["baseline_avg"] == 0.99
        assert result["per_sli"]["AV-1"]["chaos_avg"] == 0.50
        assert result["per_sli"]["AV-1"]["delta"] == pytest.approx(-0.49)


class TestComputeReliabilityScores:
    def test_full_computation(self, scorer: ReliabilityScorer) -> None:
        baseline = [
            _snap("AV-1", "availability", 0.99, True),
            _snap("FT-1", "fault_tolerance", 0.3, True),
            _snap("MA-1", "faultlessness", 0.001, True),
            _snap("RC-1", "recoverability", 0.99, True),
        ]
        chaos = [
            _snap("AV-1", "availability", 0.50, False),
            _snap("FT-1", "fault_tolerance", 0.9, False),
            _snap("MA-1", "faultlessness", 0.5, False),
            _snap("RC-1", "recoverability", 0.80, False),
        ]
        result = scorer.compute_reliability_scores(baseline, chaos)
        assert result["overall"] == 0.0
        assert result["availability"] == 0.0
        assert "details" in result
        assert "per_sli" in result["details"]
