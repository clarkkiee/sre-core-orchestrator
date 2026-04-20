"""Kubernetes service discovery for chaos experiment targeting.

Discovers application Deployments in a namespace and resolves the correct
(non-sidecar) container name for each, so chaos experiments can automatically
target the right workloads.
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
    for key in ("app", "app.kubernetes.io/name"):
        if key in match_labels:
            return f"{key}={match_labels[key]}"
    return None


def _find_app_container(containers: list[Any]) -> str | None:
    """Return the name of the first non-sidecar container, or None."""
    for container in containers:
        if container.name not in _SIDECAR_CONTAINERS:
            return str(container.name)
    return None


async def discover_services(
    kubeconfig_content: str,
    namespace: str,
) -> list[dict[str, str]]:
    """Discover application Deployments in *namespace*.

    Returns a list of dicts, each with:
        ``name``      - Deployment name (e.g. ``"frontend"``)
        ``label``     - Kubernetes label selector (e.g. ``"app=frontend"``)
        ``container`` - Application container name (e.g. ``"server"``)
    """
    api_client = await _build_api_client(kubeconfig_content)
    try:
        apps_v1 = client.AppsV1Api(api_client)
        dep_list = await apps_v1.list_namespaced_deployment(namespace)

        services: list[dict[str, str]] = []
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

            services.append(
                {"name": name, "label": app_label, "container": container_name}
            )

        logger.info(
            "Discovered %d application service(s) in ns=%s", len(services), namespace
        )
        return services
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
