import asyncio
import logging
import os
import tempfile
from http import HTTPStatus
from typing import Any

import aiohttp
import yaml
from kubernetes_asyncio import client
from kubernetes_asyncio.client import ApiException

from app.infrastructure.chaos import experiments
from app.infrastructure.chaos.exceptions import (
    ClusterNotReadyError,
    LitmusChaosExperimentError,
    LitmusCommandError,
    LitmusDeployError,
)
from app.infrastructure.config_values import get_renderer
from app.infrastructure.constants import (
    LITMUS_CRD_GROUP,
    LITMUS_CRD_VERSION,
    LITMUS_NAMESPACE,
)
from app.infrastructure.kubernetes.apply import apply_custom_object, apply_manifest
from app.infrastructure.kubernetes.client import k8s_client

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 5

class LitmusChaosManager:
    def __init__(
        self,
        kubectl_binary: str,
        litmus_version: str,
        litmus_runner_image: str = "litmuschaos/go-runner:3.27.0",
    ) -> None:
        self._kubectl_binary = kubectl_binary
        self._litmus_version = litmus_version
        self._litmus_runner_image = litmus_runner_image

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
            raise LitmusCommandError(
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
            raise LitmusCommandError(
                command=" ".join(cmd),
                returncode=proc.returncode or 1,
                stderr=stderr_text,
            )

        if stderr_text:
            logger.debug("kubectl apply stderr: %s", stderr_text)

        return stdout_text

    @staticmethod
    def _build_env(kubeconfig_path: str) -> dict[str, str]:
        env = os.environ.copy()
        env["KUBECONFIG"] = kubeconfig_path
        return env

    async def deploy(self, kubeconfig_content: str) -> None:
        tmp = tempfile.NamedTemporaryFile(  # noqa: SIM115
            mode="w", suffix=".yaml", delete=False
        )

        try:
            tmp.write(kubeconfig_content)
            tmp.flush()
            tmp.close()
            kc = tmp.name

            try:
                await self._run_kubectl(
                    "get",
                    "deployment",
                    "chaos-operator-ce",
                    "-n",
                    LITMUS_NAMESPACE,
                    kubeconfig_path=kc,
                )
            except LitmusCommandError:
                logger.info("Litmus operator not found, installing")
            else:
                logger.info("Litmus operator already deployed, skipping")
                return

            # Install operator from manifest
            manifest_url = f"https://litmuschaos.github.io/litmus/litmus-operator-v{self._litmus_version}.yaml"

            await self._run_kubectl(
                "apply",
                "-f",
                manifest_url,
                kubeconfig_path=kc,
            )

            # Deploy chaos-exporter
            cex_manifests = get_renderer().render_to_dicts(
                "litmus/chaos-exporter.yaml.j2",
                namespace=LITMUS_NAMESPACE,
            )
            for manifest in cex_manifests:
                await self._run_kubectl_apply_stdin(
                    kubeconfig_path=kc,
                    manifest_yaml=yaml.safe_dump(manifest)
                )

            # Poll until chaos-operator-ce deployment is available
            elapsed = 0
            timeout = 120
            while elapsed < timeout:
                try:
                    out = await self._run_kubectl(
                        "get",
                        "deployment",
                        "chaos-operator-ce",
                        "-n",
                        LITMUS_NAMESPACE,
                        "-o",
                        "jsonpath={.status.availableReplicas}",
                        kubeconfig_path=kc,
                    )
                    if out.strip() and int(out.strip()) > 0:
                        logger.info("Litmus operator is ready")
                        return
                except (LitmusCommandError, ValueError):
                    pass

                await asyncio.sleep(_POLL_INTERVAL)
                elapsed += _POLL_INTERVAL

            msg = f"Litmus operator not ready after {timeout}s"
            raise LitmusDeployError(msg)

        finally:
            os.unlink(tmp.name)  # noqa: PTH108 - Path.unlink() triggers ASYNC240

    async def verify_operator(self, kubeconfig_content: str) -> bool:
        async with k8s_client(kubeconfig_content) as api_client:
            try:
                apps_v1 = client.AppsV1Api(api_client)
                dep = await asyncio.wait_for(
                    apps_v1.read_namespaced_deployment(
                        name="chaos-operator-ce", namespace=LITMUS_NAMESPACE
                    ),
                    timeout=15.0
                )

                available = dep.status.available_replicas or 0

            except (TimeoutError, ApiException, aiohttp.ClientError):
                return False
            else:
                return bool(available)

    async def setup_experiment_rbac(
        self, kubeconfig_content: str, namespace: str
    ) -> None:
        renderer = get_renderer()
        async with k8s_client(kubeconfig_content) as api_client:
            # Litmus RBAC
            for manifest in renderer.render_to_dicts(
                "litmus/rbac.yaml.j2", namespace=namespace
            ):
                await apply_manifest(api_client, manifest)

            # Litmus Chaos Experiments
            for exp_type in experiments.experiment_names():
                exp_config = experiments.get_experiment(exp_type)
                body = renderer.render_to_dicts(
                    "litmus/chaos-experiment.yaml.j2",
                    experiment_type=exp_type,
                    namespace=namespace,
                    litmus_image=self._litmus_runner_image,
                    args=exp_config["args"],
                    env_vars=experiments.build_experiment_env_vars(exp_type),
                    needs_runtime_socket=experiments.needs_runtime_socket(exp_type)
                )[0]

                await apply_custom_object(api_client, body, plural="chaosexperiments")

            logger.info("Litmus RBAC + experiment templates ready in ns=%s", namespace)

    async def create_experiment(  # noqa: PLR0913
        self,
        kubeconfig_content: str,
        namespace: str,
        engine_name: str,
        app_label: str,
        experiment_type: str,
        duration: int,
        configuration: dict[str, Any] | None = None,
        probes: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        body = get_renderer().render_to_dicts(
            "litmus/chaos-engine.yaml.j2",
            engine_name=engine_name,
            namespace=namespace,
            app_label=app_label,
            experiment_type=experiment_type,
            env_vars=experiments.build_engine_env_vars(
                experiment_type, duration, configuration
            ),
            probes=probes,
        )[0]

        async with k8s_client(kubeconfig_content) as api_client:
            custom = client.CustomObjectsApi(api_client)
            result: dict[str, Any] = await custom.create_namespaced_custom_object(
                namespace=namespace,
                body=body,
                group=LITMUS_CRD_GROUP,
                version=LITMUS_CRD_VERSION,
                plural="chaosengines",
            )
            logger.info(
                "ChaosEngine %s created in ns=%s with %d probe(s)",
                engine_name,
                namespace,
                len(probes) if probes else 0,
            )
            return result

    async def poll_experiment_result(
        self,
        kubeconfig_content: str,
        namespace: str,
        engine_name: str,
        experiment_type: str,
        timeout: int,  # noqa: ASYNC109
    ) -> dict[str, Any]:
        result_name = f"{engine_name}-{experiment_type}"

        async with k8s_client(kubeconfig_content) as api_client:
            custom = client.CustomObjectsApi(api_client)
            elapsed = 0
            while elapsed < timeout:
                try:
                    result: dict[str, Any] = await custom.get_namespaced_custom_object(
                        namespace=namespace,
                        group=LITMUS_CRD_GROUP,
                        version=LITMUS_CRD_VERSION,
                        plural="chaosresults",
                        name=result_name,
                    )

                    verdict = (
                        result.get("status", {})
                        .get("experimentStatus", {})
                        .get("verdict")
                    )

                    if verdict and verdict != "Awaited":
                        logger.info("ChaosResult %s COMPLETE with verdict=%s", result_name, verdict)
                        return result

                except ApiException as e:
                    if e.status != HTTPStatus.NOT_FOUND:
                        raise

                await asyncio.sleep(_POLL_INTERVAL)
                elapsed += _POLL_INTERVAL

            msg = f"Experiment {result_name} did not complete within {timeout}s"
            raise LitmusChaosExperimentError(msg)


    async def delete_experiment(
        self,
        kubeconfig_content: str,
        namespace: str,
        engine_name: str,
    ) -> None:

        async with k8s_client(kubeconfig_content) as api_client:
            try:
                custom = client.CustomObjectsApi(api_client)
                await custom.delete_namespaced_custom_object(
                    group=LITMUS_CRD_GROUP,
                    version=LITMUS_CRD_VERSION,
                    namespace=namespace,
                    plural="chaosengines",
                    name=engine_name,
                )
                logger.info("ChaosEngine %s deleted from ns=%s", engine_name, namespace)

            except ApiException as e:
                if e.status != HTTPStatus.NOT_FOUND:
                    raise
                logger.warning("ChaosEngine %s already gone", engine_name)


    async def ensure_cluster_ready(
        self,
        kubeconfig_content: str,
        namespace: str,
        target_label: str,
        timeout_s: int,
        interval_s: int,
        require_no_active_engine: bool = True
    ) -> None:

        deadline = asyncio.get_event_loop().time() + timeout_s
        last_reason = "unknown"

        while asyncio.get_event_loop().time() < deadline:
            async with k8s_client(kubeconfig_content) as api_client:
                try:
                    # nodes ready check
                    core_v1 = client.CoreV1Api(api_client)
                    nodes = await asyncio.wait_for(core_v1.list_node(), timeout=10.0)
                    not_ready_nodes = [
                        n.metadata.name
                        for n in nodes.items
                        if not any(
                            c.type == "Ready" and c.status == "True"
                            for c in (n.status.conditions or [])
                        )
                    ]

                    if not_ready_nodes:
                        last_reason = f"nodes not ready: {not_ready_nodes}"
                        await asyncio.sleep(interval_s)
                        continue

                    # target pods ready check
                    pods = await asyncio.wait_for(
                        core_v1.list_namespaced_pod(
                            namespace=namespace,
                            label_selector=target_label
                        ),
                        timeout=10.0
                    )
                    if not pods.items:
                        last_reason = f"no pods matching label: '{target_label}'"
                        await asyncio.sleep(interval_s)
                        continue

                    not_ready_pods = [
                        p.metadata.name
                        for p in pods.items
                        if not any(
                            c.type == "Ready" and c.status == "True"
                            for c in (p.status.conditions or [])
                        )
                    ]

                    if not_ready_pods:
                        last_reason = f"pods not ready: {not_ready_pods}"
                        await asyncio.sleep(interval_s)
                        continue


                    # no active chaos engine
                    if require_no_active_engine:
                        custom = client.CustomObjectsApi(api_client)
                        engines = await asyncio.wait_for(
                            custom.list_namespaced_custom_object(
                                namespace=namespace,
                                version=LITMUS_CRD_VERSION,
                                group=LITMUS_CRD_GROUP,
                                plural="chaosengines"
                            ),
                            timeout=10.0
                        )

                        active = [
                            e["metadata"]["name"]
                            for e in engines.get("items", [])
                            if e.get("status", {}).get("engineStatus")
                            not in (None, "completed", "stopped")
                        ]

                        if active:
                            last_reason = f"Active ChaoEngines: {active}"
                            await asyncio.sleep(interval_s)
                            continue

                    logger.info(
                        "Cluster ready: namespace=%s target_label=%s",
                        namespace, target_label
                    )
                    return

                except (TimeoutError, ApiException, aiohttp.ClientError) as e:
                    last_reason = f"API error: {e}"
                    await asyncio.sleep(interval_s)

        raise ClusterNotReadyError(last_reason, timeout_s)
