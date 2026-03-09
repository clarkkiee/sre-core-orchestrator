import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.observability import (
    MetricSnapshot,
    ObservabilitySession,
    ReliabilityReport,
)


class ObservabilityRepository:
    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    # Observability Session
    async def create_session(
        self, session: ObservabilitySession
    ) -> ObservabilitySession:
        self.db.add(session)
        await self.db.flush()
        await self.db.refresh(session)
        return session

    async def get_session_by_id(
        self, session_id: uuid.UUID
    ) -> ObservabilitySession | None:
        result = await self.db.execute(
            select(ObservabilitySession).where(ObservabilitySession.id == session_id),
        )

        return result.scalar_one_or_none()

    async def list_sessions_by_tenant(
        self, tenant_id: uuid.UUID, cluster_id: uuid.UUID | None = None
    ) -> list[ObservabilitySession]:
        stmt = (
            select(ObservabilitySession)
            .where(ObservabilitySession.tenant_id == tenant_id)
            .order_by(ObservabilitySession.created_at.desc())
        )

        if cluster_id is not None:
            stmt = stmt.where(ObservabilitySession.cluster_id == cluster_id)

        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    async def update_session(
        self, session: ObservabilitySession, **fields: object
    ) -> ObservabilitySession:
        for key, value in fields.items():
            setattr(session, key, value)
        await self.db.flush()
        await self.db.refresh(session)
        return session

    # Metric Snapshot
    async def create_snapshot(self, snapshot: MetricSnapshot) -> MetricSnapshot:
        self.db.add(snapshot)
        await self.db.flush()
        await self.db.refresh(snapshot)
        return snapshot

    async def create_snapshots_bulk(self, snapshots: list[MetricSnapshot]) -> None:
        self.db.add_all(snapshots)
        await self.db.flush()

    async def get_snapshots_by_session(
        self, session_id: uuid.UUID
    ) -> list[MetricSnapshot]:
        stmt = (
            select(MetricSnapshot)
            .where(MetricSnapshot.session_id == session_id)
            .order_by(MetricSnapshot.collected_at)
        )

        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    # Reliability Report
    async def create_report(self, report: ReliabilityReport) -> ReliabilityReport:
        self.db.add(report)
        await self.db.flush()
        await self.db.refresh(report)
        return report

    async def get_report_by_id(self, report_id: uuid.UUID) -> ReliabilityReport | None:
        result = await self.db.execute(
            select(ReliabilityReport).where(ReliabilityReport.id == report_id)
        )

        return result.scalar_one_or_none()

    async def list_reports_by_tenant(
        self,
        tenant_id: uuid.UUID,
    ) -> list[ReliabilityReport]:
        stmt = (
            select(ReliabilityReport)
            .where(ReliabilityReport.tenant_id == tenant_id)
            .order_by(ReliabilityReport.created_at.desc())
        )
        result = await self.db.execute(stmt)
        return list(result.scalars().all())
