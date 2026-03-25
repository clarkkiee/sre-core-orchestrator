"""Chaos Engineering Reliability Testing Platform - Main Application."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.db.session import close_db, init_db
from app.routers.auth import router as auth_router
from app.routers.chaos import router as chaos_router
from app.routers.clusters import router as clusters_router
from app.routers.deployments import router as deployments_router
from app.routers.observability import router as observability_router
from app.routers.users import router as users_router


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
    await init_db()
    yield
    await close_db()


app = FastAPI(
    title="Chaos Platform Core",
    description="Chaos Engineering Reliability Testing Platform - Backend API",
    version="1.0.0",
    lifespan=lifespan,
    openapi_url="/openapi.json",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure via environment in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

api_router = APIRouter(prefix="/api/v1")

api_router.include_router(auth_router)
api_router.include_router(users_router)
api_router.include_router(clusters_router)
api_router.include_router(deployments_router)
api_router.include_router(observability_router)
api_router.include_router(chaos_router)


@api_router.get("/health")
async def health_check() -> dict[str, str]:
    """Health check endpoint for container orchestration."""
    return {"status": "healthy"}


@api_router.get("/")
async def root() -> dict[str, str]:
    """Root endpoint."""
    return {"message": "Chaos Platform Core API", "version": "1.0.0"}


app.include_router(api_router)
