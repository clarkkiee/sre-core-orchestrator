"""Lightweight cluster reachability check."""

import logging
import tempfile

from kubernetes_asyncio import client, config
from kubernetes_asyncio.client import ApiClient, Configuration

logger = logging.getLogger(__name__)


class ClusterHealthChecker:
    """Quick reachability probe for a Kind cluster's API server."""

    async def check_reachable(
        self,
        kubeconfig_content: str,
    ) -> tuple[bool, str]:
        """Return (is_reachable, detail_message).

        Attempts a single API server version call. Returns quickly
        on connection refused / timeout rather than polling.
        """
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                suffix=".yaml",
                delete=True,
            ) as tmp:
                tmp.write(kubeconfig_content)
                tmp.flush()
                await config.load_kube_config(config_file=tmp.name)

            configuration = Configuration.get_default_copy()
            configuration.verify_ssl = False
            configuration.ssl_ca_cert = None

            api_client = ApiClient(configuration=configuration)
            version_api = client.VersionApi(api_client=api_client)
            try:
                version_info = await version_api.get_code()
                return True, f"API server reachable (v{version_info.git_version})"
            finally:
                await api_client.close()

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
