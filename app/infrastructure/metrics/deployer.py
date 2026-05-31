"""Deploy the monitoring stack into a KinD cluster."""

import asyncio
import logging
from typing import Any

import httpx
from kubernetes_asyncio import client
from kubernetes_asyncio.client import ApiClient
from kubernetes_asyncio.client.exceptions import ApiException
from app.infrastructure.kubernetes.client import k8s_client

from app.infrastructure.config_values import get_renderer

logger = logging.getLogger(__name__)

_MONITORING_NS = "monitoring"
_POLL_INTERVAL = 5
_DEFAULT_TIMEOUT = 120
_HEALTH_RETRIES = 12
_HEALTH_DELAY = 5
_HTTP_OK = 200
_HTTP_CONFLICT = 409
_HTTP_NOT_FOUND = 404


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
        v1 = client.CoreV1Api(api_client)
        try:
            await v1.create_namespace(body=ns) # type: ignore[unused-ignore]
            logger.info("Created namespace %s", _MONITORING_NS)
        except ApiException as exc:
            if exc.status == _HTTP_CONFLICT:
                logger.info("Namespace %s already exists", _MONITORING_NS)
            else:
                raise
    
    async def _apply_blackbox_exporter(self, api_client: ApiClient) -> None:
        manifests = get_renderer().render_to_dicts(
            "monitoring/blackbox-exporter.yaml.j2", bbe_image=self._bbe_image
        )
        
        for m in manifests:
            await self._apply_manifest(api_client, m)
        logger.info("prometheus-blackbox-exporter manifests applied")
        
    async def _apply_kube_state_metrics(self, api_client: ApiClient) -> None:
        manifests = get_renderer().render_to_dicts(
        "monitoring/kube-state-metrics.yaml.j2", ksm_image=self._ksm_image
        )
        
        for m in manifests:
            await self._apply_manifest(api_client, m)
        logger.info("kube-state-metrics manifests applied")
        
    async def _apply_victoriametrics(self, api_client: ApiClient) -> None:
        manifests = get_renderer().render_to_dicts(
            "monitoring/victoria-metrics.yaml.j2", 
            vm_image=self._vm_image,
            vm_nodeport=self._vm_nodeport
        )
        
        for m in manifests:
            await self._apply_manifest(api_client, m)
        logger.info("victoriametrics manifests applied")
        
    async def _apply_manifest(
        self,
        api_client: ApiClient,
        manifest: dict[str, Any],
    ) -> None:
        """Create or update a single Kubernetes resource."""
        kind = manifest["kind"]
        name = manifest["metadata"]["name"]
        namespace = manifest["metadata"].get("namespace")

        v1 = client.CoreV1Api(api_client)
        apps_v1 = client.AppsV1Api(api_client)
        rbac_v1 = client.RbacAuthorizationV1Api(api_client)

        try:
            if kind == "ServiceAccount":
                await v1.create_namespaced_service_account(
                    namespace=namespace,
                    body=manifest,  # type: ignore[unused-ignore]
                )
            elif kind == "ClusterRole":
                await rbac_v1.create_cluster_role(body=manifest)  # type: ignore[unused-ignore]
            elif kind == "ClusterRoleBinding":
                await rbac_v1.create_cluster_role_binding(body=manifest)  # type: ignore[unused-ignore]
            elif kind == "ConfigMap":
                await v1.create_namespaced_config_map(
                    namespace=namespace,
                    body=manifest,  # type: ignore[unused-ignore]
                )
            elif kind == "Deployment":
                await apps_v1.create_namespaced_deployment(
                    namespace=namespace,
                    body=manifest,  # type: ignore[unused-ignore]
                )
            elif kind == "Service":
                await v1.create_namespaced_service(
                    namespace=namespace,
                    body=manifest,  # type: ignore[unused-ignore]
                )
            else:
                msg = f"Unsupported manifest kind: {kind}"
                raise MonitoringDeployError(msg)

            logger.info("Created %s/%s", kind, name)

        except ApiException as exc:
            if exc.status != _HTTP_CONFLICT:
                raise
            logger.info("%s/%s already exists, updating", kind, name)
            await self._update_manifest(api_client, manifest, kind, name, namespace)

    async def _update_manifest(
        self,
        api_client: ApiClient,
        manifest: dict[str, Any],
        kind: str,
        name: str,
        namespace: str | None,
    ) -> None:
        """Replace an existing resource with the new manifest."""
        v1 = client.CoreV1Api(api_client)
        apps_v1 = client.AppsV1Api(api_client)
        rbac_v1 = client.RbacAuthorizationV1Api(api_client)

        ns = namespace or _MONITORING_NS

        if kind == "ClusterRole":
            await rbac_v1.replace_cluster_role(
                name=name,
                body=manifest,  # type: ignore[unused-ignore]
            )
        elif kind == "ClusterRoleBinding":
            await rbac_v1.replace_cluster_role_binding(
                name=name,
                body=manifest,  # type: ignore[unused-ignore]
            )
        elif kind == "ConfigMap":
            await v1.replace_namespaced_config_map(
                name=name,
                namespace=ns,
                body=manifest,  # type: ignore[unused-ignore]
            )
        elif kind == "Deployment":
            await apps_v1.replace_namespaced_deployment(
                name=name,
                namespace=ns,
                body=manifest,  # type: ignore[unused-ignore]
            )
        elif kind == "Service":
            # Services need the existing clusterIP and resourceVersion
            existing = await v1.read_namespaced_service(name=name, namespace=ns)
            manifest["metadata"]["resourceVersion"] = existing.metadata.resource_version
            manifest["spec"]["clusterIP"] = existing.spec.cluster_ip
            await v1.replace_namespaced_service(
                name=name,
                namespace=ns,
                body=manifest,  # type: ignore[unused-ignore]
            )
        # ServiceAccount: skip update — no meaningful fields to change

    # -- readiness polling -----------------------------------------------------

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
                namespace=_MONITORING_NS,
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
                if resp.status_code == _HTTP_OK:
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
