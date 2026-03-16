from unittest.mock import AsyncMock, patch

import httpx
import pytest

from app.infrastructure.metrics.client import (
    MetricsConnectionError,
    VictoriaMetricsClient,
)


@pytest.fixture
def client() -> VictoriaMetricsClient:
    return VictoriaMetricsClient(base_url="http://localhost:8428")


class TestCheckHealth:
    @pytest.mark.asyncio
    async def test_healthy(self, client: VictoriaMetricsClient) -> None:
        with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = httpx.Response(200)
            ok, msg = await client.check_health()
            assert ok is True
            assert "healthy" in msg

    @pytest.mark.asyncio
    async def test_unreachable(self, client: VictoriaMetricsClient) -> None:
        with patch("httpx.AsyncClient.get", side_effect=httpx.ConnectError("")):
            ok, msg = await client.check_health()
            assert ok is False
            assert "Refuse" in msg


class TestInstantQuery:
    @pytest.mark.asyncio
    async def test_success(self, client: VictoriaMetricsClient) -> None:
        mock_body = {
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [{"metric": {}, "value": [1709000000, "0.99"]}],
            },
        }

        mock_resp = httpx.Response(200, json=mock_body)
        with patch(
            "httpx.AsyncClient.get", new_callable=AsyncMock, return_value=mock_resp
        ):
            resp = await client.instant_query(
                'avg(kube_pod_status_ready{namespace="default"})'
            )
            value = client.extract_scalar(response=resp)
            assert value == 0.99

    @pytest.mark.asyncio
    async def test_query_error(self, client: VictoriaMetricsClient) -> None:
        with (
            patch("httpx.AsyncClient.get", side_effect=httpx.ConnectError("")),
            pytest.raises(MetricsConnectionError),
        ):
            await client.instant_query("up")


class TestExtractScalar:
    def test_vector_result(self, client: VictoriaMetricsClient) -> None:
        resp = {
            "data": {
                "resultType": "vector",
                "result": [{"metric": {}, "value": [1709000000, "42.5"]}],
            }
        }
        assert client.extract_scalar(resp) == 42.5

    def test_scalar_result(self, client: VictoriaMetricsClient) -> None:
        resp = {"data": {"resultType": "scalar", "result": [1709000000, "0.95"]}}
        assert client.extract_scalar(resp) == 0.95

    def test_empty_result(self, client: VictoriaMetricsClient) -> None:
        resp = {"data": {"resultType": "vector", "result": []}}
        assert client.extract_scalar(resp) is None

    def test_malformed_result(self, client: VictoriaMetricsClient) -> None:
        resp = {"data": {"resultType": "vector", "result": [{"bad": "data"}]}}
        assert client.extract_scalar(resp) is None
