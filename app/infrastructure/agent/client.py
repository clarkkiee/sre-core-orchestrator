"""Async HTTP client for the Go provisioning agent REST API."""

from __future__ import annotations

import logging
from typing import Any

import httpx

from app.infrastructure.agent.exceptions import AgentUnreachableError

logger = logging.getLogger(__name__)


class AgentClient:
    """HTTP client for communicating with the provisioning agent."""

    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        timeout: float = 30.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._timeout = timeout
        self._http: httpx.AsyncClient | None = None

    async def _client(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                base_url=self._base_url,
                headers={"Authorization": f"Bearer {self._token}"},
                timeout=self._timeout,
            )
        return self._http

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        if self._http and not self._http.is_closed:
            await self._http.aclose()

    # ---- Health ----

    async def health(self) -> bool:
        """Return True if the agent is healthy, False otherwise."""
        try:
            client = await self._client()
            resp = await client.get("/health", timeout=3.0)
            return resp.status_code == 200  # noqa: PLR2004
        except httpx.HTTPError:
            return False

    async def health_info(self) -> dict[str, Any]:
        """Return the full health response dict. Raises on failure."""
        client = await self._client()
        resp = await client.get("/health", timeout=3.0)
        resp.raise_for_status()
        return resp.json()

    # ---- Tasks ----

    async def submit_provision(
        self,
        *,
        task_id: str,
        cluster_name: str,
        worker_count: int = 2,
        vm_cpus: int = 2,
        vm_memory: str = "2G",
        vm_disk: str = "10G",
        vm_image: str = "22.04",
        disable_traefik: bool = True,
    ) -> None:
        """POST /tasks/provision. Raises on non-202."""
        client = await self._client()
        resp = await client.post(
            "/tasks/provision",
            json={
                "task_id": task_id,
                "cluster_name": cluster_name,
                "worker_count": worker_count,
                "vm_cpus": vm_cpus,
                "vm_memory": vm_memory,
                "vm_disk": vm_disk,
                "vm_image": vm_image,
                "disable_traefik": disable_traefik,
            },
        )
        if resp.status_code != 202:  # noqa: PLR2004
            body = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
            msg = body.get("error", resp.text)
            raise AgentUnreachableError(
                self._base_url, 0, f"provision rejected ({resp.status_code}): {msg}",
            )
        logger.info("Provision task %s submitted to agent", task_id)

    async def submit_teardown(
        self,
        *,
        task_id: str,
        cluster_name: str,
        worker_count: int = 2,
    ) -> None:
        """POST /tasks/teardown. Raises on non-202."""
        client = await self._client()
        resp = await client.post(
            "/tasks/teardown",
            json={
                "task_id": task_id,
                "cluster_name": cluster_name,
                "worker_count": worker_count,
            },
        )
        if resp.status_code != 202:  # noqa: PLR2004
            body = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
            msg = body.get("error", resp.text)
            raise AgentUnreachableError(
                self._base_url, 0, f"teardown rejected ({resp.status_code}): {msg}",
            )
        logger.info("Teardown task %s submitted to agent", task_id)

    async def get_task(self, task_id: str) -> dict[str, Any]:
        """GET /tasks/{task_id}. Returns the task status dict."""
        client = await self._client()
        resp = await client.get(f"/tasks/{task_id}")
        resp.raise_for_status()
        return resp.json()

    async def cancel_task(self, task_id: str) -> None:
        """DELETE /tasks/{task_id}/cancel."""
        client = await self._client()
        resp = await client.delete(f"/tasks/{task_id}/cancel")
        resp.raise_for_status()

    # ---- VMs ----

    async def get_vm(self, vm_name: str) -> dict[str, Any] | None:
        """GET /vms/{name}. Returns VM info dict, or None if 404."""
        client = await self._client()
        resp = await client.get(f"/vms/{vm_name}")
        if resp.status_code == 404:  # noqa: PLR2004
            return None
        resp.raise_for_status()
        return resp.json()

    async def list_vms(self) -> list[dict[str, Any]]:
        """GET /vms. Returns list of VM info dicts."""
        client = await self._client()
        resp = await client.get("/vms")
        resp.raise_for_status()
        return resp.json().get("vms", [])
