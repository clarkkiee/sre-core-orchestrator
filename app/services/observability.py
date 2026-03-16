import uuid
from typing import Any

from app.exceptions.errors import ConflictError, NotFoundError
from app.models.cluster import ClusterStatus
from app.models.job import Job, JobStatus, JobType
from app.models.observability import (
    CollectionPhase,
    ObservabilitySession,
    ReliabilityReport,
    SessionStatus,
)
from app.repositories.cluster import ClusterRepository
from app.repositories.deployment import DeploymentRepository
from app.repositories.job import JobRepository
from app.repositories.observability import ObservabilityRepository
from app.schemas.observability import (
    GenerateReportRequest,
    MetricSnapshotResponse,
    ReliabilityReportResponse,
    SessionListResponse,
    SessionMetricsResponse,
    SessionResponse,
    SessionWithJobResponse,
    StartCollectionRequest,
)


class ObservabilityService:
    def __init__(
        self,
        observability_repository: ObservabilityRepository,
        cluster_repository: ClusterRepository,
        deployment_repository: DeploymentRepository,
        job_repository: JobRepository,
    ) -> None:
        self.observability_repository = observability_repository
        self.cluster_repository = cluster_repository
        self.deployment_repository = deployment_repository
        self.job_repository = job_repository

    async def start_collection(
        self,
        tenant_id: uuid.UUID,
        payload: StartCollectionRequest,
    ) -> SessionWithJobResponse:
        # Validate Cluster
        cluster_id = uuid.UUID(payload.cluster_id)
        cluster = await self.cluster_repository.get_by_id(cluster_id)
        if not cluster or cluster.tenant_id != tenant_id:
            msg = "Cluster not found"
            raise NotFoundError(msg)
        if cluster.status != ClusterStatus.READY:
            msg = f"Cluster is not ready (status: {cluster.status.value})"
            raise ConflictError(msg)
        if not cluster.victoriametrics_url:
            msg = "Cluster does not have VictoriaMetrics configured"
            raise ConflictError(msg)

        # Validate Deployment
        deployment_id = uuid.UUID(payload.deployment_id)
        deployment = await self.deployment_repository.get_by_id(deployment_id)
        if not deployment or deployment.tenant_id != tenant_id:
            msg = "Deployment not found"
            raise NotFoundError(msg)
        if deployment.cluster_id != cluster_id:
            msg = "Deployment does not belong to the specified cluster"
            raise ConflictError(msg)

        # Validate Phase
        try:
            phase = CollectionPhase(payload.phase)
        except ValueError as exc:
            msg = (
                f"Invalid phase: {payload.phase}. Must be BASELINE, CHAOS, or RECOVERY"
            )
            raise ConflictError(msg) from exc

        # Map JobType according to phase
        job_type_map = {
            CollectionPhase.BASELINE: JobType.COLLECT_BASELINE_METRICS,
            CollectionPhase.CHAOS: JobType.COLLECT_CHAOS_METRICS,
            CollectionPhase.RECOVERY: JobType.COLLECT_RECOVERY_METRICS,
        }
        job_type = job_type_map[phase]

        # Create ObservabilitySession
        session = ObservabilitySession(
            tenant_id=tenant_id,
            cluster_id=cluster_id,
            deployment_id=deployment_id,
            phase=phase,
            status=SessionStatus.PENDING,
            target_namespace=payload.target_namespace,
            collection_duration_seconds=payload.collection_duration_seconds,
            collection_interval_seconds=payload.collection_interval_seconds,
        )

        session = await self.observability_repository.create_session(session)

        # Create Job
        job = Job(
            tenant_id=tenant_id,
            cluster_id=cluster_id,
            deployment_id=deployment_id,
            job_type=job_type,
            status=JobStatus.PENDING,
        )

        job = await self.job_repository.create(job)

        # Dispatch Job as Celery Task
        from app.tasks import collect_metrics_task

        celery_result = collect_metrics_task.delay(str(session.id), str(job.id))
        await self.job_repository.update(job, celery_task_id=celery_result.id)

        return SessionWithJobResponse(
            **self._session_to_fields(session),
            job_id=str(job.id),
            job_status=job.status.value,
        )

    async def get_session(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
    ) -> SessionResponse:
        session = await self.observability_repository.get_session_by_id(session_id)
        if not session or session.tenant_id != tenant_id:
            msg = "Observability session not found"
            raise NotFoundError(msg)
        return SessionResponse(**self._session_to_fields(session))

    async def list_sessions(
        self,
        tenant_id: uuid.UUID,
        cluster_id: uuid.UUID,
    ) -> SessionListResponse:
        sessions = await self.observability_repository.list_sessions_by_tenant(
            tenant_id=tenant_id, cluster_id=cluster_id
        )

        items = [SessionResponse(**self._session_to_fields(s)) for s in sessions]
        return SessionListResponse(sessions=items, total=len(items))

    async def get_session_metrics(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
    ) -> SessionMetricsResponse:
        session = await self.observability_repository.get_session_by_id(session_id)
        if not session or session.tenant_id != tenant_id:
            msg = "Observability session not found"
            raise NotFoundError(msg)

        snapshots = await self.observability_repository.get_snapshots_by_session(
            session_id
        )
        items = [
            MetricSnapshotResponse(
                sli_name=s.sli_name,
                display_name=s.display_name,
                sub_characteristics=s.sub_characteristics,
                value=s.value,
                unit=s.unit,
                slo_target=s.slo_target,
                slo_operator=s.slo_operator,
                slo_met=s.slo_met,
                collected_at=s.collected_at,
            )
            for s in snapshots
        ]

        return SessionMetricsResponse(
            session_id=str(session_id),
            phase=session.phase.value,
            snapshots=items,
            total=len(items),
        )

    async def generate_report(
        self,
        tenant_id: uuid.UUID,
        payload: GenerateReportRequest,
    ) -> ReliabilityReportResponse:
        from app.infrastructure.metrics.scoring import ReliabilityScorer

        # Validate all sessions exist and belong to tenant
        baseline = await self._get_validated_session(
            tenant_id=tenant_id, session_id_str=payload.baseline_session_id
        )
        chaos = await self._get_validated_session(
            tenant_id=tenant_id, session_id_str=payload.chaos_session_id
        )
        recovery = await self._get_validated_session(
            tenant_id=tenant_id, session_id_str=payload.recovery_session_id
        )

        # Validate session are COMPLETED
        for s, label in [
            (baseline, "BASELINE"),
            (chaos, "CHAOS"),
            (recovery, "RECOVERY"),
        ]:
            if s.status != SessionStatus.COMPLETED:
                msg = f"{label} session is not completed (status: {s.status.value})"
                raise ConflictError(msg)

        # Load snapshots
        baseline_snapshots = (
            await self.observability_repository.get_snapshots_by_session(baseline.id)
        )
        chaos_snapshots = await self.observability_repository.get_snapshots_by_session(
            chaos.id
        )
        recovery_snapshots = (
            await self.observability_repository.get_snapshots_by_session(recovery.id)
        )

        # Compute Scores
        scorer = ReliabilityScorer()
        scores = scorer.compute_reliability_scores(
            baseline_snapshots=baseline_snapshots,
            chaos_snapshots=chaos_snapshots,
            recovery_snapshots=recovery_snapshots,
        )

        # Create report
        report = ReliabilityReport(
            tenant_id=tenant_id,
            cluster_id=uuid.UUID(payload.cluster_id),
            deployment_id=uuid.UUID(payload.deployment_id),
            baseline_session_id=baseline.id,
            chaos_session_id=chaos.id,
            recovery_session_id=recovery.id,
            overall_score=scores["overall"],
            availability_score=scores["availability"],
            faultlessness_score=scores["faultlessness"],
            fault_tolerance_score=scores["fault_tolerance"],
            recoverability_score=scores["recoverability"],
            details=scores["details"],
        )

        report = await self.observability_repository.create_report(report)

        return ReliabilityReportResponse(**self._report_to_fields(report))

    async def get_report(
        self,
        tenant_id: uuid.UUID,
        report_id: uuid.UUID,
    ) -> ReliabilityReportResponse:
        report = await self.observability_repository.get_report_by_id(report_id)
        if not report or report.tenant_id != tenant_id:
            msg = "Reliability report not found"
            raise NotFoundError(msg)
        return ReliabilityReportResponse(**self._report_to_fields(report))

    async def _get_validated_session(
        self,
        tenant_id: uuid.UUID,
        session_id_str: str,
    ) -> ObservabilitySession:
        session_id = uuid.UUID(session_id_str)
        session = await self.observability_repository.get_session_by_id(session_id)
        if not session or session.tenant_id != tenant_id:
            msg = f"Session {session_id_str} not found"
            raise NotFoundError(msg)
        return session

    @staticmethod
    def _session_to_fields(session: ObservabilitySession) -> dict[str, Any]:
        return {
            "id": str(session.id),
            "cluster_id": str(session.cluster_id),
            "deployment_id": str(session.deployment_id),
            "phase": session.phase.value,
            "status": session.status.value,
            "status_message": session.status_message,
            "target_namespace": session.target_namespace,
            "collection_duration_seconds": session.collection_duration_seconds,
            "collection_interval_seconds": session.collection_interval_seconds,
            "started_at": session.started_at,
            "completed_at": session.completed_at,
            "created_at": session.created_at,
            "updated_at": session.updated_at,
        }

    @staticmethod
    def _report_to_fields(report: ReliabilityReport) -> dict[str, Any]:
        return {
            "id": str(report.id),
            "cluster_id": str(report.cluster_id),
            "deployment_id": str(report.deployment_id),
            "baseline_session_id": str(report.baseline_session_id),
            "chaos_session_id": str(report.chaos_session_id),
            "recovery_session_id": str(report.recovery_session_id),
            "overall_score": report.overall_score,
            "availability_score": report.availability_score,
            "faultlessness_score": report.faultlessness_score,
            "fault_tolerance_score": report.fault_tolerance_score,
            "recoverability_score": report.recoverability_score,
            "details": report.details,
            "created_at": report.created_at,
        }
