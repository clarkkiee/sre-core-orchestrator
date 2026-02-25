"""Post-provisioning cluster verification using kubernetes-asyncio."""

import asyncio
import logging

from kubernetes_asyncio import client, config
from kubernetes_asyncio.client import ApiClient, Configuration

logger = logging.getLogger(__name__)

_NODE_READY = "Ready"


class KubernetesVerifier:
    """Verifies that a Kind cluster is healthy and all nodes are Ready."""

    async def verify_cluster_ready(
        self,
        kubeconfig_path: str,
        timeout_seconds: int = 120,
        poll_interval: int = 5,
    ) -> bool:
        """Poll the cluster until all nodes report Ready or timeout."""
        logger.info(
            "Waiting for cluster nodes to be ready (kubeconfig=%s, timeout=%ds)",
            kubeconfig_path,
            timeout_seconds,
        )

        elapsed = 0
        last_error: Exception | None = None

        while elapsed < timeout_seconds:
            try:
                ready = await self._check_nodes_ready(kubeconfig_path)
                if ready:
                    logger.info("All cluster nodes are Ready")
                    return True
            except Exception as exc:
                last_error = exc
                logger.warning("Node check attempt failed: %s", exc)

            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

        msg = (
            f"Cluster nodes not ready after {timeout_seconds}s"
            f"{f': {last_error}' if last_error else ''}"
        )
        raise TimeoutError(msg)

    async def _check_nodes_ready(self, kubeconfig_path: str) -> bool:
        await config.load_kube_config(config_file=kubeconfig_path)

        # Disable SSL verification at the client level because the Kind API
        # server certificate is issued for 127.0.0.1/localhost, but we connect
        # via the container's Docker network IP.
        configuration = Configuration.get_default_copy()
        configuration.verify_ssl = False
        configuration.ssl_ca_cert = None

        api_client = ApiClient(configuration=configuration)
        v1 = client.CoreV1Api(api_client=api_client)
        try:
            node_list = await v1.list_node()
            if not node_list.items:
                return False
            for node in node_list.items:
                conditions = node.status.conditions or []
                node_ready = any(
                    c.type == _NODE_READY and c.status == "True" for c in conditions
                )
                if not node_ready:
                    logger.debug(
                        "Node %s not ready yet",
                        node.metadata.name,
                    )
                    return False
            return True
        finally:
            await api_client.close()
