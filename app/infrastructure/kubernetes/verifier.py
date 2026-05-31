"""Post-provisioning cluster verification using kubernetes-asyncio."""

import asyncio
import logging

from kubernetes_asyncio import client
from app.infrastructure.kubernetes.client import k8s_client

logger = logging.getLogger(__name__)

_NODE_READY = "Ready"


class KubernetesVerifier:
    """Verifies that a Kind cluster is healthy and all nodes are Ready."""

    async def verify_cluster_ready(
        self,
        kubeconfig_content: str,
        timeout_seconds: int = 120,
        poll_interval: int = 5,
    ) -> bool:
        """Poll the cluster until all nodes report Ready or timeout."""
        logger.info(
            "Waiting for cluster nodes to be ready (timeout=%ds)",
            timeout_seconds,
        )

        elapsed = 0
        last_error: Exception | None = None

        while elapsed < timeout_seconds:
            try:
                ready = await self._check_nodes_ready(kubeconfig_content)
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

    async def _check_nodes_ready(self, kubeconfig_content: str) -> bool:
        # kubernetes-asyncio requires a file path, so write to a temp file
        async with k8s_client(kubeconfig_content) as api_client:
            v1 = client.CoreV1Api(api_client=api_client)
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
