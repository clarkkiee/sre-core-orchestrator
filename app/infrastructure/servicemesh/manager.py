import asyncio
import logging
import os
import tempfile
from http import HTTPStatus

import httpx

from app.infrastructure.servicemesh.exceptions import (
    LinkerdCommandError,
    LinkerdDeployError,
)
from app.utils.config import settings

logger = logging.getLogger(__name__)

_LINKERD_NS = "linkerd"
_LINKERD_VIZ_NS = "linkerd-viz"
_GATEWAY_API_BASE_URL = (
    "https://github.com/kubernetes-sigs/gateway-api/releases/download"
)
_CHECK_RETRIES = 3
_CHECK_RETRY_DELAY = 10
_GATEWAY_INSTALL_RETRIES = 3
_GATEWAY_RETRY_DELAY = 2
_VIZ_WAIT_TIMEOUT = "120s"
_VIZ_OPTIONS = [
    "--set",
    "prometheus.enabled=false",
    "--set",
    "prometheusUrl=http://victoria-metrics.monitoring.svc.cluster.local:8428",
    "--set",
    "grafana.enabled=false",
    "--set",
    "dashboard.enforcedHostRegexp=.*",
]


class LinkerdManager:
    """Manages Linkerd service mesh lifecycle"""

    def __init__(
        self,
        linkerd_binary: str,
        kubectl_binary: str,
        gateway_api_version: str,
    ) -> None:
        self._linkerd_binary = linkerd_binary
        self._kubectl_binary = kubectl_binary
        self._gateway_api_version = gateway_api_version

    def _build_env(self, kubeconfig_path: str) -> dict[str, str]:
        """Build an environment dict"""
        env = os.environ.copy()
        env["KUBECONFIG"] = kubeconfig_path
        path = env.get("PATH", "")
        for p in ("/usr/local/bin", "/usr/bin", "/bin"):
            if p not in path:
                path = f"{p}:{path}"
        env["PATH"] = path
        if settings.DOCKER_HOST:
            env["DOCKER_HOST"] = settings.DOCKER_HOST
        return env

    async def _run_linkerd(self, *args: str, kubeconfig_path: str) -> str:
        cmd = [self._linkerd_binary, *args]
        logger.info("Running: %s", " ".join(cmd))

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=self._build_env(kubeconfig_path),
        )

        stdout, stderr = await proc.communicate()

        stdout_text = stdout.decode().strip()
        stderr_text = stderr.decode().strip()

        if proc.returncode != 0:
            raise LinkerdCommandError(
                command=" ".join(cmd),
                returncode=proc.returncode or 1,
                stderr=stderr_text,
            )

        if stderr_text:
            logger.debug("linkerd stderr: %s", stderr_text)

        return stdout_text

    async def _run_kubectl(self, *args: str, kubeconfig_path: str) -> str:
        cmd = [self._kubectl_binary, *args]
        logger.info("Running: %s", " ".join(cmd))

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=self._build_env(kubeconfig_path),
        )
        stdout, stderr = await proc.communicate()

        stdout_text = stdout.decode().strip()
        stderr_text = stderr.decode().strip()

        if proc.returncode != 0:
            raise LinkerdCommandError(
                command=" ".join(cmd),
                returncode=proc.returncode or 1,
                stderr=stderr_text,
            )

        if stderr_text:
            logger.debug("kubectl stderr: %s", stderr_text)

        return stdout_text

    async def _run_kubectl_apply_stdin(
        self, manifest_yaml: str, kubeconfig_path: str
    ) -> str:
        cmd = [self._kubectl_binary, "apply", "-f", "-"]
        logger.info("Running %s (stdin manifest)", " ".join(cmd))

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=self._build_env(kubeconfig_path),
        )
        stdout, stderr = await proc.communicate(input=manifest_yaml.encode())

        stdout_text = stdout.decode().strip()
        stderr_text = stderr.decode().strip()

        if proc.returncode != 0:
            raise LinkerdCommandError(
                command=" ".join(cmd),
                returncode=proc.returncode or 1,
                stderr=stderr_text,
            )

        if stderr_text:
            logger.debug("kubectl apply stderr: %s", stderr_text)

        return stdout_text

    async def _install_gateway_crds(self, kc: str) -> None:
        """Download and apply Gateway API CRDs"""
        gateway_url = (
            f"{_GATEWAY_API_BASE_URL}/{self._gateway_api_version}/standard-install.yaml"
        )

        for attempt in range(1, _GATEWAY_INSTALL_RETRIES + 1):
            try:
                async with httpx.AsyncClient(timeout=10, follow_redirects=True) as http:
                    resp = await http.get(gateway_url)
                if resp.status_code == HTTPStatus.OK:
                    await self._run_kubectl_apply_stdin(
                        kubeconfig_path=kc, manifest_yaml=resp.text
                    )
                    logger.info("Gateway API CRDs successfully retrieved")
                    return
                logger.warning(
                    "Gateway API CRDs cannot be retrieved, attempt %d status: %d",
                    attempt,
                    resp.status_code,
                )
            except httpx.HTTPError as e:
                logger.warning(
                    "Gateway API CRDs cannot be retrieved, attempt %d failed: %s",
                    attempt,
                    e,
                )

            if attempt < _GATEWAY_INSTALL_RETRIES:
                await asyncio.sleep(_GATEWAY_RETRY_DELAY)
            else:
                msg = (
                    f"Gateway API CRDs cannot be retrieved"
                    f" from {gateway_url}"
                    f" after {_GATEWAY_INSTALL_RETRIES} attempts"
                )
                raise LinkerdDeployError(msg)

    async def _install_viz(self, kc: str) -> None:
        """Install Linkerd Viz extension"""
        viz_yaml = await self._run_linkerd(
            "viz", "install", *_VIZ_OPTIONS, kubeconfig_path=kc
        )
        await self._run_kubectl_apply_stdin(manifest_yaml=viz_yaml, kubeconfig_path=kc)
        await self._run_kubectl(
            "wait",
            "--for=condition=Available",
            "--timeout=" + _VIZ_WAIT_TIMEOUT,
            "deployment",
            "--all",
            "-n",
            _LINKERD_VIZ_NS,
            kubeconfig_path=kc,
        )
        await self._run_linkerd("viz", "check", kubeconfig_path=kc)
        logger.info("Linkerd Viz Installation Success")

    async def deploy(self, kubeconfig_content: str) -> None:
        """Deploy Linkerd service mesh into the cluster"""

        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as tmp:
            tmp.write(kubeconfig_content.encode())
            tmp.flush()
            kc = tmp.name

        try:
            # Idempotency check, skip if already healthy
            try:
                await self._run_kubectl("get", "ns", _LINKERD_NS, kubeconfig_path=kc)
                await self._run_linkerd("check", kubeconfig_path=kc)
            except LinkerdCommandError:
                logger.info("Linkerd is not yet installed")
            else:
                logger.info("Linkerd already installed and healthy")
                return

            # Gateway API CRDs
            await self._install_gateway_crds(kc=kc)

            # Pre-Installation check
            await self._run_linkerd("check", "--pre", kubeconfig_path=kc)
            logger.info("Linkerd Pre-Installation Check Passed")

            # Install Linkerd CRDs
            crd_yaml = await self._run_linkerd("install", "--crds", kubeconfig_path=kc)
            await self._run_kubectl_apply_stdin(
                manifest_yaml=crd_yaml, kubeconfig_path=kc
            )
            logger.info("Linkerd CRD Installation Success")

            # Install Linkerd Control Plane
            control_plane_yaml = await self._run_linkerd("install", kubeconfig_path=kc)
            await self._run_kubectl_apply_stdin(
                manifest_yaml=control_plane_yaml, kubeconfig_path=kc
            )
            logger.info("Linkerd Control Plane Installation Success")

            # Post-Installation Check
            for attempt in range(1, _CHECK_RETRIES + 1):
                try:
                    await self._run_linkerd("check", kubeconfig_path=kc)
                    logger.info("Linkerd Post-Installation check Passed")
                    break
                except LinkerdCommandError:
                    if attempt < _CHECK_RETRIES:
                        logger.warning(
                            "Post-Installation check attempt %d failed, retrying...",
                            attempt,
                        )
                        await asyncio.sleep(_CHECK_RETRY_DELAY)
                    else:
                        raise

            # Install Linkerd Viz
            await self._install_viz(kc=kc)

        except LinkerdCommandError as e:
            msg = f"Linkerd deployment failed: {e}"
            raise LinkerdDeployError(msg) from e
        finally:
            os.unlink(tmp.name)  # noqa: PTH108 - Path.unlink() triggers ASYNC240

    async def inject_namespaces(
        self, kubeconfig_path: str, namespaces: list[str]
    ) -> None:
        try:
            for ns in namespaces:
                await self._run_kubectl(
                    "annotate",
                    "ns",
                    ns,
                    "linkerd.io/inject=enabled",
                    "--overwrite",
                    kubeconfig_path=kubeconfig_path,
                )
                # Restart existing workloads so the admission webhook
                # injects the proxy sidecar into already-running pods.
                await self._run_kubectl(
                    "rollout",
                    "restart",
                    "deployment",
                    "-n",
                    ns,
                    kubeconfig_path=kubeconfig_path,
                )
                await self._run_kubectl(
                    "rollout",
                    "status",
                    "deployment",
                    "-n",
                    ns,
                    "--timeout=600s",
                    kubeconfig_path=kubeconfig_path,
                )
            logger.info("Enabling Namespace Injection Success")
        except LinkerdCommandError as e:
            msg = f"Linkerd Namespaces Injection failed: {e}"
            raise LinkerdDeployError(msg) from e
