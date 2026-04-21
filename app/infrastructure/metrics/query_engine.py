import logging
import uuid
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from app.infrastructure.metrics.catalog import MetricCatalog, MetricDefinition
from app.infrastructure.metrics.client import VictoriaMetricsClient
from app.infrastructure.metrics.client_factory import VictoriaMetricsClientFactory
from app.models.raw_metric_sample import MetricPhase
from app.repositories.raw_metric_sample import RawMetricSampleRepository

logger = logging.getLogger(__name__)

@dataclass
class MetricFetchResult:
    metric_name: str
    sample_count: int = 0
    series_count: int = 0
    error: str | None = None

@dataclass
class PhaseFetchReport:
    experiment_id: uuid.UUID
    phase: MetricPhase
    total_samples: int = 0
    per_metric: list[MetricFetchResult] = field(default_factory=list)

def parse_range_response(
    resp: dict[str, Any],
    labels_to_keep: Sequence[str],
) -> Iterator[tuple[dict[str, str], datetime, float]]:

    labels_set = set(labels_to_keep)
    for series in resp.get("data", {}).get("result", []):
        metrics_label = series.get("metric", {}) or {}
        kept = {k: v for k, v in metrics_label.items() if k in labels_set}

        for pair in series.get("values", []) or []:
            if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                continue
            ts_raw, val_raw = pair
            try:
                ts = datetime.fromtimestamp(float(ts_raw), tz=UTC)
                val = float(val_raw)
            except (TypeError, ValueError):
                continue
            yield kept, ts, val

class MetricsQueryEngine:
    def __init__(
        self,
        client_factory: VictoriaMetricsClientFactory,
        catalog: MetricCatalog,
        repo: RawMetricSampleRepository
    ) -> None:
        self._client_factory = client_factory
        self._catalog = catalog
        self._repo = repo

    async def fetch_phase( # noqa: PLR0913
        self,
        experiment_id: uuid.UUID,
        phase: MetricPhase,
        namespace: str,
        start: datetime,
        end: datetime,
        metric_names: list[str] | None = None,
        extra_params: dict[str, str] | None = None,
    ) -> PhaseFetchReport:
        client = await self._client_factory.for_experiment(experiment_id)

        targets = self._resolve_targets(metric_names)
        params = {"ns": namespace, **(extra_params or {})}

        buffered_rows: list[dict] = []
        report = PhaseFetchReport(experiment_id=experiment_id, phase=phase)

        for definition in targets:
            result = MetricFetchResult(metric_name=definition.name)
            try:
                rows = await self._fetch_one(
                    client=client,
                    definition=definition, params=params,
                    start=start, end=end
                )
                result.sample_count = len(rows)
                result.series_count = len({
                    tuple(sorted(r["labels"].items())) for r in rows
                })

                for r in rows:
                    r["experiment_id"] = experiment_id
                    r["phase"] = phase
                buffered_rows.extend(rows)

            except Exception as e:
                logger.warning(
                    "metric fetch failed: %s (experiment=%s phase=%s): %s",
                    definition.name, experiment_id, phase, e
                )
                result.error = str(e)
            report.per_metric.append(result)

        if buffered_rows:
            inserted = await self._repoe.bulk_insert(buffered_rows)
            report.total_samples = inserted

        return report

    def _resolve_targets(
        self, metric_names: list[str] | None
    ) -> list[MetricDefinition]:
        if metric_names is None:
            return self._catalog.all()
        return [self._catalog.get(n) for n in metric_names]

    async def _fetch_one(
        self,
        client: VictoriaMetricsClient,
        definition: MetricDefinition,
        params: dict[str, str],
        start: datetime,
        end: datetime,
    ) -> list[dict]:
        promql = definition.render(params)
        resp = await client.range_query(
            promql=promql, end=end, start=start, step=definition.default_step
        )
        return [
            {
                "metric_name": definition.name,
                "source": definition.source,
                "labels": labels,
                "timestamp": ts,
                "value": value
            }
            for labels, ts, value in parse_range_response(
                resp=resp,
                labels_to_keep=definition.labels_to_keep
            )
        ]
