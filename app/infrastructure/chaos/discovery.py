"""Kubernetes service discovery for chaos experiment targeting.

Discovers application Deployments and Services in a namespace and resolves the
correct (non-sidecar) container name, service port, and likely protocol for
each target so chaos experiments can automatically pick the right probe style.
"""

from __future__ import annotations

import logging
import tempfile
from typing import Any

from kubernetes_asyncio import client, config
from kubernetes_asyncio.client import ApiClient, Configuration

logger = logging.getLogger(__name__)

# Container names injected by service-mesh sidecars — never chaos-targeted.
_SIDECAR_CONTAINERS: set[str] = {
    "linkerd-proxy",
    "linkerd-init",
    "istio-proxy",
    "istio-init",
    "envoy-sidecar",
}

# Deployment name prefixes that belong to infrastructure, not the AUT.
_INFRA_PREFIXES: tuple[str, ...] = (
    "linkerd",
    "litmus",
    "chaos-operator",
    "victoria",
    "kube-state",
    "blackbox",
)


async def _build_api_client(kubeconfig_content: str) -> ApiClient:
    """Build a kubernetes_asyncio ApiClient from raw kubeconfig text."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=True) as tmp:
        tmp.write(kubeconfig_content)
        tmp.flush()
        await config.load_kube_config(config_file=tmp.name)

    configuration = Configuration.get_default_copy()
    configuration.verify_ssl = False
    configuration.ssl_ca_cert = None
    return ApiClient(configuration=configuration)


def _extract_app_label(match_labels: dict[str, str] | None) -> str | None:
    """Return the first usable app label selector from Deployment matchLabels."""
    if not match_labels:
        return None
    for key in ("app", "app.kubernetes.io/name", "name"):
        if key in match_labels:
            return f"{key}={match_labels[key]}"
    return None


def _find_app_container(containers: list[Any]) -> str | None:
    """Return the name of the first non-sidecar container, or None."""
    for container in containers:
        if container.name not in _SIDECAR_CONTAINERS:
            return str(container.name)
    return None


def _selector_matches(
    selector: dict[str, str] | None,
    labels: dict[str, str] | None,
) -> bool:
    if not selector or not labels:
        return False
    return all(labels.get(key) == value for key, value in selector.items())


def _infer_service_protocol(service_name: str, service_port: Any) -> str:
    """Infer the most likely app protocol from a Service port."""
    port_name = getattr(service_port, "name", "") or ""
    app_protocol = getattr(service_port, "app_protocol", "") or ""
    joined = f"{service_name} {port_name} {app_protocol}".lower()
    if "grpc" in joined:
        return "grpc"
    if "http" in joined or "https" in joined:
        return "http"
    return "tcp"


def _pick_service_port(service: Any) -> tuple[int, str, str] | None:
    """Return (port, protocol, clusterIP) for the best service port."""
    ports = service.spec.ports or []
    if not ports:
        return None

    cluster_ip = service.spec.cluster_ip or ""
    preferred_port = None
    for port in ports:
        if _infer_service_protocol(service.metadata.name, port) in {"http", "grpc"}:
            preferred_port = port
            break

    chosen = preferred_port or ports[0]
    return (
        int(chosen.port),
        _infer_service_protocol(service.metadata.name, chosen),
        cluster_ip,
    )


async def discover_services(
    kubeconfig_content: str,
    namespace: str,
) -> list[dict[str, Any]]:
    """Discover application Deployments in *namespace*.

    Returns a list of dicts, each with:
        ``name``      - Deployment name (e.g. ``"frontend"``)
        ``label``     - Kubernetes label selector (e.g. ``"app=frontend"``)
        ``container`` - Application container name (e.g. ``"server"``)
        ``port``      - Target Service port (e.g. ``80``)
        ``protocol``  - Likely app protocol (``http``, ``grpc`` or ``tcp``)
    """
    api_client = await _build_api_client(kubeconfig_content)
    try:
        apps_v1 = client.AppsV1Api(api_client)
        core_v1 = client.CoreV1Api(api_client)
        dep_list = await apps_v1.list_namespaced_deployment(namespace)
        svc_list = await core_v1.list_namespaced_service(namespace)

        services: list[dict[str, Any]] = []
        for dep in dep_list.items:
            name: str = dep.metadata.name

            # Skip infrastructure deployments.
            if name.startswith(_INFRA_PREFIXES):
                continue

            match_labels = (
                dep.spec.selector.match_labels if dep.spec.selector else None
            )
            app_label = _extract_app_label(match_labels)
            if not app_label:
                logger.warning(
                    "Skipping deployment %s — no usable app label found", name
                )
                continue

            containers = dep.spec.template.spec.containers or []
            container_name = _find_app_container(containers)
            if not container_name:
                logger.warning(
                    "Skipping deployment %s — no non-sidecar container found", name
                )
                continue

            dep_labels = dep.spec.selector.match_labels if dep.spec.selector else None
            selected_service = None
            for svc in svc_list.items:
                if _selector_matches(svc.spec.selector, dep_labels):
                    picked = _pick_service_port(svc)
                    if picked:
                        selected_service = {
                            "port": picked[0],
                            "protocol": picked[1],
                            "clusterIP": picked[2],
                        }
                        break

            if not selected_service:
                logger.warning(
                    "Skipping deployment %s — no matching Service with a usable port found",
                name,
                )
                continue

            services.append(
                {
                    "name": name,
                    "label": app_label,
                    "container": container_name,
                    "port": selected_service["port"],
                    "protocol": selected_service["protocol"],
                    "clusterIP": selected_service["clusterIP"],
                }
            )

        logger.info(
            "Discovered %d application service(s) in ns=%s", len(services), namespace
        )
        return services
    finally:
        await api_client.close()


async def resolve_service_target(
    kubeconfig_content: str,
    namespace: str,
    label_selector: str,
) -> dict[str, Any] | None:
    """Resolve the Service port/protocol for a deployment label selector."""
    api_client = await _build_api_client(kubeconfig_content)
    try:
        apps_v1 = client.AppsV1Api(api_client)
        core_v1 = client.CoreV1Api(api_client)
        dep_list = await apps_v1.list_namespaced_deployment(namespace)
        svc_list = await core_v1.list_namespaced_service(namespace)

        for dep in dep_list.items:
            match_labels = dep.spec.selector.match_labels if dep.spec.selector else None
            if _extract_app_label(match_labels) != label_selector:
                continue

            for svc in svc_list.items:
                if _selector_matches(svc.spec.selector, match_labels):
                    picked = _pick_service_port(svc)
                    if not picked:
                        return None
                    return {
                        "name": svc.metadata.name,
                        "port": picked[0],
                        "protocol": picked[1],
                        "clusterIP": picked[2],
                    }

        return None
    finally:
        await api_client.close()


async def resolve_target_container(
    kubeconfig_content: str,
    namespace: str,
    label_selector: str,
) -> str | None:
    """Resolve the application container name for pods matching *label_selector*.

    Returns the first non-sidecar container name, or ``None`` if no pods or
    no suitable container is found.
    """
    api_client = await _build_api_client(kubeconfig_content)
    try:
        v1 = client.CoreV1Api(api_client)
        pods = await v1.list_namespaced_pod(
            namespace, label_selector=label_selector
        )
        if not pods.items:
            return None

        pod = pods.items[0]
        return _find_app_container(pod.spec.containers or [])
    finally:
        await api_client.close()
