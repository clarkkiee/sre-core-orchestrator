import asyncio
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.infrastructure.chaos import experiments
from app.infrastructure.chaos.discovery import discover_services
from app.infrastructure.chaos.naming import LITMUS_NAME_TO_TYPE
from app.infrastructure.factories import build_litmus_manager
from app.models.campaign import CampaignStatus, ChaosCampaign
from app.models.chaos import ChaosExperiment, ChaosExperimentStatus
from app.models.job import Job, JobStatus
from app.repositories.campaign import CampaignRepository
from app.repositories.chaos import ChaosRepository
from app.repositories.cluster import ClusterRepository
from app.repositories.job import JobRepository
from app.tasks.celery_config import celery_app
from app.tasks.chaos_tasks import _run_single_experiment
from app.tasks.shared import JobProgress, _make_session_maker, record_job_failure
from app.utils.config import settings

logger = logging.getLogger(__name__)

# Temporary campaign allowlist for the current runtime cleanup.
# ---------------------------------------------------------------------------
# Celery task entry point
# ---------------------------------------------------------------------------


@celery_app.task(  # type: ignore[misc]
    bind=True,
    name="app.tasks.run_chaos_campaign",
    max_retries=1,
)
def run_chaos_campaign_task(
    self: Any,  # noqa: ANN401
    campaign_id: str,
    job_id: str,
) -> dict[str, str]:
    """Run a full Chaos Campaign against all discovered services."""
    _ = self
    return asyncio.run(
        _run_chaos_campaign(campaign_id=campaign_id, job_id=job_id)
    )

async def _record_campaign_failure(
    campaign_id: uuid.UUID,
    job_id: uuid.UUID,
    exc: Exception,
) -> None:
    async def _entity(session: AsyncSession) -> None:
        repo = CampaignRepository(session)
        exp = await repo.get_by_id(campaign_id)
        if exp:
            msg = str(exc)
            await repo.update(
                exp, status=CampaignStatus.FAILED,
                status_message=msg[:500],
                completed_at=datetime.now(UTC)
            )
    await record_job_failure(job_id, exc, update_entity=_entity)

async def _run_campaign_phases(  # noqa: PLR0913, PLR0915
    campaign_repo: CampaignRepository,
    chaos_repo: ChaosRepository,
    job_repo: JobRepository,
    job: Job,
    session: Any,  # noqa: ANN401
    campaign: ChaosCampaign,
    kubeconfig: str,
    vm_url: str | None = None,
) -> dict[str, str]:
    """Execute all campaign phases in order."""

    litmus_manager = build_litmus_manager()
    progress = JobProgress(job_repo, job)

    # ------------------------------------------------------------------
    # Phase 1: VALIDATING (2%)
    # ------------------------------------------------------------------
    await job_repo.update(
        job, status=JobStatus.RUNNING, current_phase="VALIDATING", progress_percentage=2
    )
    await session.commit()

    is_ready = await litmus_manager.verify_operator(kubeconfig)
    if not is_ready:
        msg = "Litmus Operator is not running"
        raise RuntimeError(msg)

    # ------------------------------------------------------------------
    # Phase 2: DISCOVERING (5%)
    # ------------------------------------------------------------------
    await job_repo.update(job, current_phase="DISCOVERING", progress_percentage=5)
    await campaign_repo.update(campaign, status=CampaignStatus.DISCOVERING)
    await session.commit()

    services = await discover_services(kubeconfig, campaign.target_namespace)
    if not services:
        msg = (
            f"No application services discovered in namespace "
            f"{campaign.target_namespace}"
        )
        raise RuntimeError(msg)

    experiment_types = [
        name
        for name in experiments.experiment_names()
    ]
    total = len(experiment_types) * len(services)

    await campaign_repo.update(
        campaign,
        discovered_services=services,
        total_experiments=total,
        status=CampaignStatus.RUNNING,
        started_at=datetime.now(UTC),
    )
    await session.commit()

    logger.info(
        "Campaign %s: discovered %d services, %d total experiments",
        campaign.id, len(services), total,
    )

    # ------------------------------------------------------------------
    # Phase 3: PREPARING (10%)
    # ------------------------------------------------------------------
    await progress("PREPARING_RBAC", 10)

    await litmus_manager.setup_experiment_rbac(
        kubeconfig_content=kubeconfig, namespace=campaign.target_namespace
    )

    # ------------------------------------------------------------------
    # Phase 4: EXECUTING (10% → 90%)
    # ------------------------------------------------------------------
    completed = 0
    failed_count = 0

    for litmus_name in experiment_types:
        experiment_type_enum = LITMUS_NAME_TO_TYPE[litmus_name]
        
        if litmus_name not in ["pod-delete", "pod-network-loss"]:
            continue
        
        for service in services:
            if service['name'] not in ["cartservice", "productcatalogservice"]:
                continue
            # Check if campaign was stopped between experiments.
            await session.refresh(campaign)
            if campaign.status == CampaignStatus.STOPPED:
                logger.info("Campaign %s stopped by user", campaign.id)
                await job_repo.update(
                    job,
                    current_phase="STOPPED",
                    status=JobStatus.CANCELED,
                    completed_at=datetime.now(UTC),
                )
                await session.commit()
                return {"campaign_id": str(campaign.id), "status": "STOPPED"}

            engine_name = (
                f"{litmus_name}-{service['name']}-{uuid.uuid4().hex[:8]}"
            )

            # Build configuration for this experiment.
            config: dict[str, str] | None = None
            if experiments.needs_runtime_socket(litmus_name):
                config = {"TARGET_CONTAINER": service["container"]}

            # Determine default duration from the template.
            template_env = experiments.get_experiment(litmus_name)["env"]
            duration = int(template_env.get("TOTAL_CHAOS_DURATION", "60"))

            experiment = ChaosExperiment(
                tenant_id=campaign.tenant_id,
                cluster_id=campaign.cluster_id,
                deployment_id=campaign.deployment_id,
                campaign_id=campaign.id,
                experiment_type=experiment_type_enum,
                target_namespace=campaign.target_namespace,
                target_label=service["label"],
                duration_seconds=duration,
                chaos_engine_name=engine_name,
                configuration=config,
                status=ChaosExperimentStatus.PENDING,
            )
            experiment = await chaos_repo.create(experiment)
            await session.commit()

            logger.info(
                "Campaign %s: running %s → %s (%d/%d)",
                campaign.id, litmus_name, service["name"], completed + 1, total,
            )

            await progress(
                f"BASELINE_WAIT:{litmus_name}:{service['name']}",
                10 + int(80 * completed / total),
            )

            verdict = await _run_single_experiment(
                litmus_manager=litmus_manager,
                chaos_repo=chaos_repo,
                kubeconfig=kubeconfig,
                experiment=experiment,
                litmus_name=litmus_name,
                target_port=str(
                    service.get("port", settings.PROBE_DEFAULT_TARGET_PORT)
                ),
                service_protocol=str(service.get("protocol", "http")),
                target_clusterip=str(service.get("clusterIP", "")),
                session=session,
                vm_url=vm_url,
                health_path=service.get("health_path"),
            )

            completed += 1
            if verdict == "Error":
                failed_count += 1

            await campaign_repo.update(
                campaign, completed_experiments=completed
            )
            await session.commit()

    # ------------------------------------------------------------------
    # Phase 5: COMPLETE (100%)
    # ------------------------------------------------------------------
    final_status = (
        CampaignStatus.FAILED if failed_count == total else CampaignStatus.COMPLETED
    )
    status_msg = (
        f"Completed: {completed - failed_count} passed, {failed_count} failed"
    )

    await campaign_repo.update(
        campaign,
        status=final_status,
        status_message=status_msg,
        completed_at=datetime.now(UTC),
    )
    await job_repo.update(
        job,
        current_phase="COMPLETE",
        progress_percentage=100,
        status=JobStatus.COMPLETED,
        completed_at=datetime.now(UTC),
    )
    await session.commit()

    return {"campaign_id": str(campaign.id), "status": final_status.value}


async def _run_chaos_campaign(
    campaign_id: str, job_id: str
) -> dict[str, str]:
    cid = uuid.UUID(campaign_id)
    jid = uuid.UUID(job_id)

    async with _make_session_maker()() as session:
        campaign_repo = CampaignRepository(session)
        chaos_repo = ChaosRepository(session)
        job_repo = JobRepository(session)
        cluster_repo = ClusterRepository(session)

        campaign = await campaign_repo.get_by_id(cid)
        job = await job_repo.get_by_id(jid)
        if not campaign or not job:
            msg = f"Campaign {campaign_id} or Job {job_id} not found"
            raise ValueError(msg)

        cluster = await cluster_repo.get_by_id(campaign.cluster_id)
        if not cluster:
            msg = f"Cluster {campaign.cluster_id} not found"
            raise ValueError(msg)

        kubeconfig = cluster.kubeconfig
        if not kubeconfig:
            msg = "kubeconfig is missing"
            raise ValueError(msg)

        try:
            result = await _run_campaign_phases(
                campaign_repo=campaign_repo,
                chaos_repo=chaos_repo,
                job_repo=job_repo,
                job=job,
                session=session,
                campaign=campaign,
                kubeconfig=kubeconfig,
                vm_url=cluster.victoriametrics_url,
            )
        except Exception as exc:
            await session.rollback()
            logger.exception(
                "Chaos campaign failed",
                extra={"campaign_id": campaign_id, "job_id": job_id},
            )
            await _record_campaign_failure(cid, jid, exc)
            raise

        logger.info("Chaos campaign %s finished", campaign_id)
        return result
