from fastapi import APIRouter

from app.dependencies import CurrentUser, AnalyticsServiceDep
from app.schemas.analytics import AnalyticResponse
from app.exceptions.errors import UnauthorizedError, error_responses

router = APIRouter(prefix="/analytics", tags=["analytics"])

@router.get(
    "",
    response_model=AnalyticResponse,
    responses=error_responses(
        (UnauthorizedError, "Invalid or expired token")
    )
)
async def get_analytics(
    current_user: CurrentUser,
    analytics_service: AnalyticsServiceDep
) -> AnalyticResponse:
    return await analytics_service.get_stats(current_user)
    