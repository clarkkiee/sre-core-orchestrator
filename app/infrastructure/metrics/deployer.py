"""Deploy the monitoring stack into a KinD cluster."""

import asyncio
import logging
from http import HTTPStatus
from typing import Any

import httpx
from kubernetes_asyncio import client
from kubernetes_asyncio.client import ApiClient

from app.infrastructure.config_values import get_renderer
from app.infrastructure.constants import MONITORING_NAMESPACE
from app.infrastructure.kubernetes.apply import apply_manifest
from app.infrastructure.kubernetes.client import k8s_client

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 5
_DEFAULT_TIMEOUT = 120
_HEALTH_RETRIES = 12
_HEALTH_DELAY = 5

class MonitoringDeployError(Exception):
    """Raised when the monitoring stack fails to deploy."""


class MonitoringStackDeployer:
    """Deploys VictoriaMetrics + kube-state-metrics into a cluster."""

    def __init__(
        self, vm_image: str, ksm_image: str, vm_nodeport: int, bbe_image: str
    ) -> None:
        self._vm_image = vm_image
        self._ksm_image = ksm_image
        self._vm_nodeport = vm_nodeport
        self._bbe_image = bbe_image

    async def deploy(
        self,
        kubeconfig_content: str,
        control_plane_ip: str,
    ) -> str:
        async with k8s_client(kubeconfig_content) as api_client:
            await self._apply_namespace(api_client)
            await self._apply_kube_state_metrics(api_client)
            await self._apply_blackbox_exporter(api_client)
            await self._apply_victoriametrics(api_client)

            logger.info("Waiting for monitoring pods to become ready")
            await self._wait_for_ready(
                api_client,
                label_selector="app=kube-state-metrics",
            )
            await self._wait_for_ready(
                api_client,
                label_selector="app=blackbox-exporter",
            )
            await self._wait_for_ready(
                api_client,
                label_selector="app=victoria-metrics",
            )

        host = control_plane_ip
        vm_url = f"http://{host}:{self._vm_nodeport}"
        await self._health_check(vm_url)

        logger.info("Monitoring stack deployed — VM URL: %s", vm_url)
        return vm_url

    async def _apply_namespace(self, api_client: ApiClient) -> None:
        ns = get_renderer().render_to_dicts("monitoring/namespace.yaml")[0]
        await apply_manifest(api_client, ns)

    async def _apply_blackbox_exporter(self, api_client: ApiClient) -> None:
        manifests = get_renderer().render_to_dicts(
            "monitoring/blackbox-exporter.yaml.j2", bbe_image=self._bbe_image
        )

        for m in manifests:
            await apply_manifest(api_client, m)
        logger.info("prometheus-blackbox-exporter manifests applied")

    async def _apply_kube_state_metrics(self, api_client: ApiClient) -> None:
        manifests = get_renderer().render_to_dicts(
        "monitoring/kube-state-metrics.yaml.j2", ksm_image=self._ksm_image
        )

        for m in manifests:
            await apply_manifest(api_client, m)
        logger.info("kube-state-metrics manifests applied")

    async def _apply_victoriametrics(self, api_client: ApiClient) -> None:
        manifests = get_renderer().render_to_dicts(
            "monitoring/victoria-metrics.yaml.j2",
            vm_image=self._vm_image,
            vm_nodeport=self._vm_nodeport
        )

        for m in manifests:
            await apply_manifest(api_client, m)
        logger.info("victoriametrics manifests applied")

    async def _wait_for_ready(
        self,
        api_client: ApiClient,
        label_selector: str,
        timeout_seconds: int = _DEFAULT_TIMEOUT,
    ) -> None:
        """Poll until all matching pods have Ready condition."""
        v1 = client.CoreV1Api(api_client)
        elapsed = 0

        while elapsed < timeout_seconds:
            pod_list = await v1.list_namespaced_pod(
                namespace=MONITORING_NAMESPACE,
                label_selector=label_selector,
            )

            if pod_list.items:
                all_ready = all(self._pod_is_ready(pod) for pod in pod_list.items)
                if all_ready:
                    logger.info("Pods matching '%s' are ready", label_selector)
                    return

            await asyncio.sleep(_POLL_INTERVAL)
            elapsed += _POLL_INTERVAL

        msg = f"Pods matching '{label_selector}' not ready after {timeout_seconds}s"
        raise MonitoringDeployError(msg)

    @staticmethod
    def _pod_is_ready(pod: Any) -> bool:  # noqa: ANN401
        """Check if a pod has the Ready condition set to True."""
        if not pod.status or not pod.status.conditions:
            return False
        return any(
            c.type == "Ready" and c.status == "True" for c in pod.status.conditions
        )

    async def _health_check(self, vm_url: str) -> None:
        """Verify VictoriaMetrics is responding."""
        url = f"{vm_url}/health"

        for attempt in range(1, _HEALTH_RETRIES + 1):
            try:
                async with httpx.AsyncClient(timeout=10) as http:
                    resp = await http.get(url)
                if resp.status_code == HTTPStatus.OK:
                    logger.info("VictoriaMetrics health check passed")
                    return
                logger.warning(
                    "VM health check attempt %d: status %d",
                    attempt,
                    resp.status_code,
                )
            except httpx.HTTPError as exc:
                logger.warning(
                    "VM health check attempt %d failed: %s",
                    attempt,
                    exc,
                )

            if attempt < _HEALTH_RETRIES:
                await asyncio.sleep(_HEALTH_DELAY)

        msg = (
            f"VictoriaMetrics at {vm_url} not healthy after {_HEALTH_RETRIES} attempts"
        )
        raise MonitoringDeployError(msg)
