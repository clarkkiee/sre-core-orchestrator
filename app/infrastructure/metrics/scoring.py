from collections.abc import Mapping
from typing import Any

from app.infrastructure.metrics.sli_registry import get_all_sub_characteristics


class ReliabilityScorer:
    def compute_sub_characteristic_score(
        self,
        snapshots: list[Any],
        sub_characteristic: str,
    ) -> float | None:
        relevant = [
            s
            for s in snapshots
            if s.sub_characteristics == sub_characteristic and s.slo_met is not None
        ]

        if not relevant:
            return None

        met_count = sum(1 for s in relevant if s.slo_met)
        return met_count / len(relevant)

    def compute_all_scores(
        self,
        snapshots: list[Any],
    ) -> dict[str, float | None]:
        """Compute SLO compliance per sub-characteristic from snapshots."""
        return {
            sub: self.compute_sub_characteristic_score(snapshots, sub)
            for sub in get_all_sub_characteristics()
        }

    def compute_overall_score(
        self,
        scores: Mapping[str, float | None],
        weights: Mapping[str, float] | None = None,
    ) -> float:
        """Weighted average across sub-characteristics."""
        valid_scores = {k: v for k, v in scores.items() if v is not None}
        if not valid_scores:
            return 0.0

        if weights is None:
            weight = 1.0 / len(valid_scores)
            return sum(v * weight for v in valid_scores.values())

        total_weight = sum(weights.get(k, 0.0) for k in valid_scores)
        if total_weight == 0.0:
            return 0.0

        return sum(
            valid_scores[k] * weights.get(k, 0.0) / total_weight for k in valid_scores
        )

    def compute_sli_averages(
        self,
        snapshots: list[Any],
    ) -> dict[str, float | None]:
        """Compute average value per SLI across all snapshots.

        Returns {"AV-1": 0.995, "FT-1": 0.45, ...}
        """
        sli_values: dict[str, list[float]] = {}
        for s in snapshots:
            if s.value is not None:
                sli_values.setdefault(s.sli_name, []).append(s.value)

        return {
            sli: sum(vals) / len(vals) if vals else None
            for sli, vals in sli_values.items()
        }

    def compare_sessions(
        self,
        baseline_snapshots: list[Any],
        chaos_snapshots: list[Any],
    ) -> dict[str, Any]:
        """Compare baseline vs chaos: deltas and scores."""
        baseline_avgs = self.compute_sli_averages(baseline_snapshots)
        chaos_avgs = self.compute_sli_averages(chaos_snapshots)

        per_sli: dict[str, dict[str, Any]] = {}
        all_slis = set(baseline_avgs.keys()) | set(chaos_avgs.keys())
        for sli in all_slis:
            b_val = baseline_avgs.get(sli)
            c_val = chaos_avgs.get(sli)
            delta = None
            degradation_pct = None
            if b_val is not None and c_val is not None:
                delta = c_val - b_val
                if b_val != 0:
                    degradation_pct = round(abs(delta) / abs(b_val) * 100, 2)

            per_sli[sli] = {
                "baseline_avg": b_val,
                "chaos_avg": c_val,
                "delta": delta,
                "degradation_pct": degradation_pct,
            }

        baseline_scores = self.compute_all_scores(baseline_snapshots)
        chaos_scores = self.compute_all_scores(chaos_snapshots)

        per_sub: dict[str, dict[str, Any]] = {}
        for sub in get_all_sub_characteristics():
            per_sub[sub] = {
                "baseline_score": baseline_scores.get(sub),
                "chaos_score": chaos_scores.get(sub),
            }

        return {
            "per_sli": per_sli,
            "per_sub_characteristic": per_sub,
        }

    def compute_reliability_scores(
        self,
        baseline_snapshots: list[Any],
        chaos_snapshots: list[Any],
        recovery_snapshots: list[Any] | None = None,
    ) -> dict[str, Any]:
        """Full reliability evaluation for ReliabilityReport."""
        chaos_scores = self.compute_all_scores(chaos_snapshots)
        overall = self.compute_overall_score(chaos_scores)
        comparison = self.compare_sessions(baseline_snapshots, chaos_snapshots)

        if recovery_snapshots:
            recovery_scores = self.compute_all_scores(recovery_snapshots)
            comparison["recovery_scores"] = recovery_scores

        return {
            "overall": round(overall, 4),
            "availability": chaos_scores.get("availability") or 0.0,
            "faultlessness": chaos_scores.get("faultlessness") or 0.0,
            "fault_tolerance": chaos_scores.get("fault_tolerance") or 0.0,
            "recoverability": chaos_scores.get("recoverability") or 0.0,
            "details": comparison,
        }
