"""Lightweight cluster reachability check."""

import logging

from kubernetes_asyncio import client
from app.infrastructure.kubernetes.client import k8s_client

logger = logging.getLogger(__name__)


class ClusterHealthChecker:
    """Quick reachability probe for a Kind cluster's API server."""

    async def check_reachable(
        self,
        kubeconfig_content: str,
    ) -> tuple[bool, str]:
        try:
            async with k8s_client(kubeconfig_content) as api_client:
                version_api = client.VersionApi(api_client=api_client)
                version_info = await version_api.get_code()
                return True, f"API server reachable (v{version_info.git_version})"
        except Exception as exc:
            reason = str(exc)
            if "Connection refused" in reason:
                return False, (
                    "Cluster API server is not reachable (connection refused). "
                    "The underlying Docker container may have been destroyed."
                )
            if "timed out" in reason.lower() or "timeout" in reason.lower():
                return False, (
                    "Cluster API server timed out. The cluster may be unreachable."
                )
            return False, f"Cluster health check failed: {reason}"
