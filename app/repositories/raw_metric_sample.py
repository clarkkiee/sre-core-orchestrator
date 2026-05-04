import uuid
from collections.abc import Sequence
from typing import Any

from sqlalchemy import select, insert, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.raw_metric_sample import MetricPhase, RawMetricSample

_BULK_BATCH_SIZE = 500

class RawMetricSampleRepository:
    def __init__(self, db: AsyncSession) -> None:
        self._db = db

    async def bulk_insert(self, rows: Sequence[dict[str, Any]]) -> int:
        """Insert raw metric sample in batches, returns total inserted"""
        if not rows:
            return 0

        inserted = 0
        for i in range (0, len(rows), _BULK_BATCH_SIZE):
            batch = rows[i : i + _BULK_BATCH_SIZE]
            await self._db.execute(insert(RawMetricSample), batch)
            inserted += len(batch)

        await self._db.commit()
        return inserted


    async def list_by_experiment_phase(
        self,
        experiment_id: uuid.UUID,
        phase: MetricPhase,
        metric_name: str | None = None
    ) -> list[RawMetricSample]:
        stmt = select(RawMetricSample).where(
            RawMetricSample.experiment_id == experiment_id,
            RawMetricSample.phase == phase,
        )

        if metric_name is not None:
            stmt = stmt.where(RawMetricSample.metric_name == metric_name)

        stmt = stmt.order_by(RawMetricSample.timestamp)

        result = await self._db.execute(stmt)
        return list(result.scalars().all())

    async def delete_for_experiment_phase(
        self,
        experiment_id: uuid.UUID,
        phase: MetricPhase
    ) -> None:
        stmt = delete(RawMetricSample).where(
            (RawMetricSample.experiment_id == experiment_id) &
            (RawMetricSample.phase == phase)
        )

        await self._db.execute(stmt)
        await self._db.commit()
