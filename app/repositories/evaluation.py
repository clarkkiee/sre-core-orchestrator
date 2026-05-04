import uuid
from typing import Any

from sqlalchemy import insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.evaluation_indicator import EvaluationIndicator, MeasurementScope
from app.models.experiment_evaluation import EvaluationStatus, ExperimentEvaluation
from app.models.chaos import ChaosExperiment

class EvaluationRepository:
    def __init__(self, db: AsyncSession) -> None:
        self._db = db

    async def get_by_experiment(
        self, experiment_id: uuid.UUID
    ) -> ExperimentEvaluation | None:
        stmt = select(ExperimentEvaluation).where(
            ExperimentEvaluation.experiment_id == experiment_id
        )
        result = await self._db.execute(stmt)
        return result.scalar_one_or_none()

    async def upsert_evaluation(
        self, data: dict[str, Any]
    ) -> ExperimentEvaluation:
        """Insert or update the envelope row keyed on experiment_id."""
        stmt = (
            pg_insert(ExperimentEvaluation)
            .values(**data)
            .on_conflict_do_update(
                index_elements=["experiment_id"],
                set_={
                    k: v for k, v in data.items()
                    if k not in ("id", "experiment_id")
                },
            )
            .returning(ExperimentEvaluation)
        )
        result = await self._db.execute(stmt)
        await self._db.commit()
        return result.scalar_one()

    async def upsert_indicators(
        self, rows: list[dict[str, Any]]
    ) -> int:
        """Upsert evaluation_indicators rows, keyed on the unique dimension constraint."""
        if not rows:
            return 0

        stmt = (
            pg_insert(EvaluationIndicator)
            .values(rows)
            .on_conflict_do_update(
                constraint="uq_evaluation_indicator_dimensions",
                set_={
                    "value": pg_insert(EvaluationIndicator).excluded.value,
                    "sample_count": pg_insert(EvaluationIndicator).excluded.sample_count,
                    "episode_count": pg_insert(EvaluationIndicator).excluded.episode_count,
                    "extra": pg_insert(EvaluationIndicator).excluded.extra,
                    "computed_at": pg_insert(EvaluationIndicator).excluded.computed_at,
                },
            )
        )
        await self._db.execute(stmt)
        await self._db.commit()
        return len(rows)

    async def mark_failed(
        self,
        experiment_id: uuid.UUID,
        status_message: str,
        **envelope_fields: Any,
    ) -> None:
        """Upsert a FAILED evaluation row so failures are visible in the DB."""
        data: dict[str, Any] = {
            "id": uuid.uuid4(),
            "experiment_id": experiment_id,
            "status": EvaluationStatus.FAILED,
            "status_message": status_message[:2000],
            **envelope_fields,
        }
        stmt = (
            pg_insert(ExperimentEvaluation)
            .values(**data)
            .on_conflict_do_update(
                index_elements=["experiment_id"],
                set_={
                    "status": EvaluationStatus.FAILED,
                    "status_message": data["status_message"],
                },
            )
        )
        await self._db.execute(stmt)
        await self._db.commit()

    async def get_with_indicators(
        self,
        experiment_id: uuid.UUID,
        scope: MeasurementScope | None = None,
    ) -> tuple[
        ChaosExperiment | None,
        ExperimentEvaluation | None,
        list[EvaluationIndicator]
    ]:
        exp_stmt = select(ChaosExperiment).where(ChaosExperiment.id == experiment_id)
        exp_result = await self._db.execute(exp_stmt)

        experiment = exp_result.scalar_one_or_none()
        if experiment is None:
            return None, None, []

        eval_stmt = select(ExperimentEvaluation).where(
            ExperimentEvaluation.experiment_id == experiment_id
        )
        eval_result = await self._db.execute(eval_stmt)

        evaluation = eval_result.scalar_one_or_none()
        if evaluation is None:
            return experiment, None, []

        indicators_stmt = select(EvaluationIndicator).where(
            EvaluationIndicator.evaluation_id == evaluation.id
        )
        if scope is not None:
            indicators_stmt = indicators_stmt.where(EvaluationIndicator.scope == scope)

        indicators_stmt = indicators_stmt.order_by(
            EvaluationIndicator.phase,
            EvaluationIndicator.indicator
        )

        indicators_result = await self._db.execute(indicators_stmt)
        indicators = list(indicators_result.scalars().all())

        return experiment, evaluation, indicators
