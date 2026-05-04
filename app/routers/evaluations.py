import uuid
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, status

from app.dependencies import CurrentUser, EvaluationRepositoryDep
from app.models.evaluation_indicator import MeasurementScope
from app.schemas.evaluation import (
    ExperimentEvaluationResponse,
    _build_evaluation_response,
)

router = APIRouter(prefix="/chaos", tags=["evaluations"])

@router.get(
    "/experiments/{experiment_id}/evaluation",
    response_model=ExperimentEvaluationResponse,
    status_code=status.HTTP_200_OK
)
async def get_experiment_evaluation(
    experiment_id: uuid.UUID,
    user: CurrentUser,
    eval_repo: EvaluationRepositoryDep,
    scope: Annotated[MeasurementScope, Query()] = MeasurementScope.TARGET
) -> ExperimentEvaluationResponse:
    experiment, evaluation, indicators = await eval_repo.get_with_indicators(
        experiment_id=experiment_id,
        scope=scope,
    )

    if experiment is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Experiment not found"
        )

    if not user.is_admin and experiment.tenant_id != user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied"
        )

    if evaluation is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Evaluation not yet available for this experiment"
        )

    return _build_evaluation_response(experiment, evaluation, indicators, scope)
