"""Metrics API endpoints."""

from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter
from pydantic import BaseModel

from app.dependencies import MetricsQueryEngineDep
from app.models.raw_metric_sample import MetricPhase

router = APIRouter(prefix="/metrics", tags=["metrics"])

class FetchMetricsRequest(BaseModel):
    experiment_id: UUID
    phase: MetricPhase
    namespace: str
    start: datetime
    end: datetime
    metric_names: list[str] | None = None

@router.post("/fetch")
async def fetch_metrics(
    body: FetchMetricsRequest,
    engine: MetricsQueryEngineDep,
) -> dict[str, Any]:
    report = await engine.fetch_phase(
        experiment_id=body.experiment_id,
        phase=body.phase,
        namespace=body.namespace,
        start=body.start,
        end=body.end,
        metric_names=body.metric_names
    )

    return {
        "experiment_id": str(report.experiment_id),
        "phase": report.phase.value,
        "total_samples": report.total_samples,
        "per_metric": [
            {
                "metric_name": m.metric_name,
                "sample_count": m.sample_count,
                "series_count": m.series_count,
                "error": m.error
            }
            for m in report.per_metric
        ]
    }
