import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel

from app.models.chaos import ChaosExperiment
from app.models.evaluation_indicator import EvaluationIndicator
from app.models.experiment_evaluation import ExperimentEvaluation

_FV_SOURCE_MAP: dict[str, str] = {
    "blackbox_tcp": "blackbox_tcp",
    "linkerd":      "linkerd",
    "pod_ready":    "pod_ready",
}

def _source_from_fv(formula_version: str) -> str:
    for key, val in _FV_SOURCE_MAP.items():
        if key in formula_version:
            return val
    return "default"

def _format_value(indicator: str, raw: float) -> tuple[float, str]:
    match indicator:
        case "SYSTEM_AVAILABILITY":
            return round(raw * 100, 2), "%"
        case "ERROR_RATE":
            return round(raw * 100, 4), "%"
        case "CPU_UTILIZATION":
            return round(raw, 6), "cores"
        case "MEMORY_UTILIZATION":
            return round(raw / (1024 ** 2), 2), "MB"
        case "RESPONSE_TIME_P95":
            return round(raw, 3), "ms"
        case _:
            # MEAN_DOWN_TIME, MEAN_RECOVERY_TIME, MTTF = detik
            return round(raw, 3), "s"

class IndicatorEntryResponse(BaseModel):
    value: float
    unit: str
    source: str
    formula_version: str
    sub_characteristics: str
    sample_count: int
    episode_count: int
    extra: dict[str, Any] | None
    model_config = {"from_attributes": True}

class PhaseWindowResponse(BaseModel):
    start: datetime
    end: datetime

class ExperimentSummaryResponse(BaseModel):
    id: uuid.UUID
    experiment_type: str
    target_label: str
    target_namespace: str
    duration_seconds: int
    status: str
    status_message: str

    model_config = {"from_attributes": True}

class EvaluationResponse(BaseModel):
    id: uuid.UUID
    status: str
    litmus_probe_percentage: float | None
    evaluation_version: str
    evaluated_at: datetime
    baseline: PhaseWindowResponse
    fault: PhaseWindowResponse
    recovery: PhaseWindowResponse

    model_config = {"from_attributes": True}

PhaseIndicators = dict[str, list[IndicatorEntryResponse]]

class ExperimentEvaluationResponse(BaseModel):
    experiment: ExperimentSummaryResponse
    evaluation: EvaluationResponse
    scope: str
    baseline: PhaseIndicators
    fault: PhaseIndicators
    recovery: PhaseIndicators
    cross_phase: PhaseIndicators

def _build_evaluation_response(
    experiment: ChaosExperiment,
    evaluation: ExperimentEvaluation,
    indicators: list[EvaluationIndicator],
    scope: str,
) -> ExperimentEvaluationResponse:

    baseline: PhaseIndicators = {}
    fault: PhaseIndicators = {}
    recovery: PhaseIndicators = {}
    cross_phase: PhaseIndicators = {}

    for ind in indicators:
        formatted_value, unit = _format_value(ind.indicator, ind.value)
        entry = IndicatorEntryResponse(
            value=formatted_value,
            episode_count=ind.episode_count,
            extra=ind.extra,
            formula_version=ind.formula_version,
            sample_count=ind.sample_count,
            source=_source_from_fv(ind.formula_version),
            sub_characteristics=ind.sub_characteristic,
            unit=unit,
        )
        indicator_name = str(ind.indicator)

        if ind.phase is None:
            bucket = cross_phase
        elif str(ind.phase) == "BASELINE":
            bucket = baseline
        elif str(ind.phase) == "FAULT":
            bucket = fault
        else:
            bucket = recovery

        bucket.setdefault(indicator_name, []).append(entry)

    return ExperimentEvaluationResponse(
        experiment=ExperimentSummaryResponse(
            id=experiment.id,
            duration_seconds=experiment.duration_seconds,
            experiment_type=experiment.experiment_type,
            target_label=experiment.target_label,
            status=experiment.status,
            status_message=experiment.status_message,
            target_namespace=experiment.target_namespace
        ),
        evaluation=EvaluationResponse(
            id=evaluation.id,
            status=str(evaluation.status),
            litmus_probe_percentage=evaluation.litmus_probe_percentage,
            evaluation_version=evaluation.evaluator_version,
            evaluated_at=evaluation.evaluated_at,
            baseline=PhaseWindowResponse(
                start=evaluation.baseline_start,
                end=evaluation.baseline_end
            ),
            fault=PhaseWindowResponse(
                start=evaluation.fault_start,
                end=evaluation.fault_end
            ),
            recovery=PhaseWindowResponse(
                start=evaluation.recovery_start,
                end=evaluation.recovery_end
            ),
        ),
        scope=scope,
        baseline=baseline,
        fault=fault,
        recovery=recovery,
        cross_phase=cross_phase
    )