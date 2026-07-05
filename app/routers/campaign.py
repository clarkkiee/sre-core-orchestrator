"""Chaos Campaign API endpoints"""

import uuid

from fastapi import APIRouter, Depends, HTTPException, status

from app.dependencies import CampaignServiceDep, CurrentUser
from app.schemas.campaign import (
    CampaignFilterParams,
    CampaignPage,
    CampaignResponse,
    StartCampaignRequest,
    StopCampaignResponse,
)
from app.schemas.pagination import PaginationParams

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
    response_model=CampaignPage,
    status_code=status.HTTP_200_OK,
)
async def list_campaigns(
    user: CurrentUser,
    campaign_service: CampaignServiceDep,
    params: PaginationParams = Depends(),
    filters: CampaignFilterParams = Depends(),
) -> CampaignPage:
    """List chaos campaigns. Admins see all campaigns; regular users see their own."""
    return await campaign_service.list_campaigns(user, params, filters.to_filter())


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
    result = await campaign_service.get_campaign(user, campaign_id)
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
