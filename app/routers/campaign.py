"""Chaos Campaign API endpoints"""

import uuid

from fastapi import APIRouter, HTTPException, status

from app.dependencies import CampaignServiceDep, CurrentUser
from app.schemas.campaign import (
    CampaignListResponse,
    CampaignResponse,
    StartCampaignRequest,
    StopCampaignResponse,
)

router = APIRouter(prefix="/chaos", tags=["chaos-campaigns"])


@router.post(
    "/campaigns",
    response_model=CampaignResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def start_campaign(
    payload: StartCampaignRequest,
    user: CurrentUser,
    campaign_service: CampaignServiceDep,
) -> CampaignResponse:
    campaign, _ = await campaign_service.start_campaign(user.id, payload=payload)
    return campaign


@router.get(
    "/campaigns",
    response_model=CampaignListResponse,
    status_code=status.HTTP_200_OK,
)
async def list_campaigns(
    user: CurrentUser,
    campaign_service: CampaignServiceDep,
    cluster_id: uuid.UUID | None = None,
) -> CampaignListResponse:
    return await campaign_service.list_campaigns(user.id, cluster_id)


@router.get(
    "/campaigns/{campaign_id}",
    response_model=CampaignResponse,
    status_code=status.HTTP_200_OK,
)
async def get_campaign(
    campaign_id: uuid.UUID,
    user: CurrentUser,
    campaign_service: CampaignServiceDep,
) -> CampaignResponse:
    result = await campaign_service.get_campaign(user.id, campaign_id)
    if not result:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return result


@router.delete(
    "/campaigns/{campaign_id}",
    response_model=StopCampaignResponse,
)
async def stop_campaign(
    campaign_id: uuid.UUID,
    user: CurrentUser,
    campaign_service: CampaignServiceDep,
) -> StopCampaignResponse:
    result = await campaign_service.stop_campaign(user.id, campaign_id)
    if not result:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return result
