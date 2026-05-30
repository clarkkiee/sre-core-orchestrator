"""Chaos API endpoints"""

import uuid
from typing import Any

from fastapi import APIRouter, HTTPException, status

from app.dependencies import ChaosServiceDep, CurrentUser
from app.infrastructure.chaos import experiments
from app.schemas.chaos import (
    ChaosExperimentListResponse,
    ChaosExperimentResponse,
    StartChaosExperimentRequest,
    StopExperimentResponse,
)

router = APIRouter(prefix="/chaos", tags=["chaos"])


@router.post(
    "/experiments",
    response_model=ChaosExperimentResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def start_experiment(
    payload: StartChaosExperimentRequest,
    user: CurrentUser,
    chaos_service: ChaosServiceDep,
) -> ChaosExperimentResponse:
    experiment, _ = await chaos_service.start_experiment(user.id, payload=payload)
    return experiment


@router.get(
    "/experiments",
    response_model=ChaosExperimentListResponse,
    status_code=status.HTTP_200_OK,
)
async def list_experiments(
    user: CurrentUser,
    chaos_service: ChaosServiceDep,
    cluster_id: uuid.UUID | None = None,
) -> ChaosExperimentListResponse:
    return await chaos_service.list_experiment(user.id, cluster_id)


@router.get(
    "/experiments/{experiment_id}",
    response_model=ChaosExperimentResponse,
    status_code=status.HTTP_200_OK,
)
async def get_experiment(
    experiment_id: uuid.UUID, user: CurrentUser, chaos_service: ChaosServiceDep
) -> ChaosExperimentResponse:
    result = await chaos_service.get_experiment(user.id, experiment_id)
    if not result:
        raise HTTPException(status_code=404, detail="Experiment not found")
    return result


@router.delete("/experiments/{experiment_id}", response_model=StopExperimentResponse)
async def stop_experiment(
    experiment_id: uuid.UUID, user: CurrentUser, chaos_service: ChaosServiceDep
) -> StopExperimentResponse:
    exp = await chaos_service.stop_experiment(user.id, experiment_id)
    if not exp:
        raise HTTPException(status_code=404, detail="Experiment not found")
    return StopExperimentResponse(
        id=exp.id, message=exp.status_message or "Experiment Stopped", status=exp.status
    )


@router.get("/experiment-types")
async def list_experiment_types() -> list[dict[str, Any]]:
    return [
        {"name": name, "tunables": list(experiments.get_experiment(name)["env"].keys())}
        for name in experiments.experiment_names()
    ]
