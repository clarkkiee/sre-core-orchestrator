import logging
from datetime import datetime
from typing import Any

import httpx

_DEFAULT_TIMEOUT = 30.0
_HTTP_OK = 200
_SCALAR_RESULT_LENGTH = 2

logger = logging.getLogger(__name__)


class MetricsQueryError(Exception):
    def __init__(self, query: str, reason: str) -> None:
        self.query = query
        self.reason = reason
        super().__init__(f"Metrics query failed: {reason} (query={query})")


class MetricsConnectionError(Exception):
    def __init__(self, base_url: str, reason: str) -> None:
        self.base_url = base_url
        self.reason = reason
        super().__init__(f"Cannot reach metrics server at {base_url}: {reason}")


class VictoriaMetricsClient:
    """HTTP Client for Victoria Metrics"""

    def __init__(self, base_url: str, timeout: float = _DEFAULT_TIMEOUT) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    async def check_health(self) -> tuple[bool, str]:
        """Check if VictoriaMetrics is reachable"""

        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.get(f"{self._base_url}/health")
                if resp.status_code == _HTTP_OK:
                    return True, "VictoriaMetrics is healthy"
                return False, f"Unexpected err with status code: {resp.status_code}"
        except httpx.ConnectError:
            return False, "Connection Refused"
        except httpx.TimeoutException:
            return False, "Connection Timed Out"
        except Exception as e:
            return False, f"Health check failed: {e}"

    async def instant_query(self, promql: str) -> dict[str, Any]:
        params = {"query": promql}
        return await self._request("/api/v1/query", params, promql)

    async def range_query(
        self,
        promql: str,
        start: datetime,
        end: datetime,
        step: str = "15s",
    ) -> dict[str, Any]:
        """Execute a range query over a time window"""

        params = {
            "query": promql,
            "start": start.timestamp(),
            "end": end.timestamp(),
            "step": step,
        }

        return await self._request("/api/v1/query_range", params, promql)

    async def _request(
        self,
        path: str,
        params: dict[str, Any],
        promql: str,
    ) -> dict[str, Any]:
        """Send a GET Request to VictoriaMetrics API"""

        url = f"{self._base_url}{path}"

        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.get(url=url, params=params)
        except httpx.ConnectError as e:
            raise MetricsConnectionError(self._base_url, "Connection Refused") from e
        except httpx.TimeoutException as e:
            raise MetricsConnectionError(self._base_url, "Request Timed Out") from e
        except Exception as e:
            raise MetricsConnectionError(self._base_url, str(e)) from e

        body = resp.json()

        if resp.status_code != _HTTP_OK or body.get("status") != "success":
            error_msg = body.get("error", body.get("errorType", "unknown error"))
            logger.warning("PromQL query failed: %s -- %s", promql, error_msg)
            raise MetricsQueryError(query=promql, reason=error_msg)

        return body  # type: ignore[no-any-return]

    @staticmethod
    def extract_scalar(response: dict[str, Any]) -> float | None:
        """Extract a single scalar value from an instant query response"""

        data = response.get("data", {})
        result_type = data.get("resultType")
        result = data.get("result", [])

        if result_type == "scalar" and len(result) == _SCALAR_RESULT_LENGTH:
            try:
                return float(result[1])
            except (ValueError, TypeError):
                return None

        if result_type == "vector" and len(result) > 0:
            try:
                return float(result[0]["value"][1])
            except (KeyError, IndexError, ValueError, TypeError):
                return None

        return None

    @staticmethod
    def extract_vector(response: dict[str, Any]) -> list[dict[str, Any]]:
        """Extract All series from a vector query response"""

        data = response.get("data", {})
        result: list[dict[str, Any]] = data.get("result", [])
        return result
