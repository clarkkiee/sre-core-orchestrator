from http import HTTPStatus
from typing import Any

from kubernetes_asyncio import client
from kubernetes_asyncio.client import ApiClient
from kubernetes_asyncio.client.exceptions import ApiException


async def apply_manifest(api: ApiClient, manifest: dict[str, Any]) -> None:

    kind = manifest["kind"]
    name = manifest["metadata"]["name"]
    namespace = manifest["metadata"].get("namespace")

    v1 = client.CoreV1Api(api)
    apps = client.AppsV1Api(api)
    rbac = client.RbacAuthorizationV1Api(api)

    creators = {
        "Namespace": lambda: v1.create_namespace(body=manifest), # type: ignore
        "ServiceAccount": lambda: v1.create_namespaced_service_account(namespace=namespace, body=manifest), # type: ignore
        "ClusterRole": lambda: rbac.create_cluster_role(body=manifest), # type: ignore
        "ClusterRoleBinding": lambda: rbac.create_cluster_role_binding(body=manifest), # type: ignore
        "Role": lambda: rbac.create_namespaced_role(namespace=namespace, body=manifest), # type: ignore
        "RoleBinding": lambda: rbac.create_namespaced_role_binding(namespace=namespace, body=manifest), # type: ignore
        "ConfigMap": lambda: v1.create_namespaced_config_map(namespace=namespace, body=manifest), # type: ignore
        "Deployment": lambda: apps.create_namespaced_deployment(namespace=namespace, body=manifest), # type: ignore
        "Service": lambda: v1.create_namespaced_service(namespace=namespace, body=manifest) # type: ignore
    }

    if kind not in creators:
        raise ValueError(f"Unsupported manifest kind: {kind}")

    try:
        await creators[kind]()
    except ApiException as exc:
        if exc.status != HTTPStatus.CONFLICT:
            raise
        await _replace(api, manifest, kind, name, namespace)


async def _replace(api, manifest, kind, name, namespace) -> None:
    v1 = client.CoreV1Api(api)
    apps = client.AppsV1Api(api)
    rbac = client.RbacAuthorizationV1Api(api)

    replacers = {
        "ClusterRole": lambda: rbac.replace_cluster_role(name=name, body=manifest),
        "ClusterRoleBinding": lambda: rbac.replace_cluster_role_binding(name=name, body=manifest),
        "Role": lambda: rbac.replace_namespaced_role(name=name, namespace=namespace, body=manifest),
        "RoleBinding": lambda: rbac.replace_namespaced_role_binding(name=name, namespace=namespace, body=manifest),
        "ConfigMap": lambda: v1.replace_namespaced_config_map(name=name, namespace=namespace, body=manifest),
        "Deployment": lambda: apps.replace_namespaced_deployment(name=name, namespace=namespace, body=manifest)
    }

    if kind == "Service":
        existing = await v1.read_namespaced_service(name=name, namespace=namespace)
        manifest["metadata"]["resourceVersion"] = existing.metadata.resource_version
        manifest["spec"]["clusterIP"] = existing.spec.cluster_ip

        await v1.replace_namespaced_service(name=name, namespace=namespace, body=manifest)
    elif kind in replacers:
        await replacers[kind]()

async def apply_custom_object(api: ApiClient, manifest: dict[str, Any], *, plural: str) -> None:
    group, version = manifest["apiVersion"].split("/", 1)
    namespace = manifest["metadata"]["namespace"]
    name = manifest["metadata"]["name"]

    custom = client.CustomObjectsApi(api)
    try:
        await custom.create_namespaced_custom_object(
            group=group, version=version, namespace=namespace, plural=plural,
            body=manifest
        )
    except ApiException as exc:
        if exc.status != HTTPStatus.CONFLICT:
            raise
        existing = await custom.get_namespaced_custom_object(
            group=group, version=version, name=name,
            namespace=namespace, plural=plural
        )
        manifest["metadata"]["resourceVersion"] = existing["metadata"]["resourceVersion"]
        await custom.replace_namespaced_custom_object(
            group=group, version=version, namespace=namespace,
            plural=plural, name=name,
            body=manifest
        )
