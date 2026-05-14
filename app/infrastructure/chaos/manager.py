import asyncio
import logging
import os
import tempfile
from typing import Any, cast

import aiohttp
import yaml
from kubernetes_asyncio import client, config
from kubernetes_asyncio.client import ApiClient, ApiException, Configuration

from app.infrastructure.chaos.exceptions import (
    LitmusChaosExperimentError,
    LitmusCommandError,
    LitmusDeployError,
    ClusterNotReadyError,
)
from app.infrastructure.chaos.manifests import (
    EXPERIMENT_TEMPLATES,
    build_chaos_engine,
    build_chaos_experiment,
    build_chaos_exporter,
    build_namespaced_litmuschaos_rbac,
)

logger = logging.getLogger(__name__)

_LITMUS_NS = "litmus"
_LITMUS_CRD_GROUP = "litmuschaos.io"
_LITMUS_CRD_VERSION = "v1alpha1"
_POLL_INTERVAL = 5
_HTTP_CONFLICT = 409
_HTTP_NOT_FOUND = 404


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

    @staticmethod
    async def _build_api_client(kubeconfig_content: str) -> ApiClient:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=True) as tmp:
            tmp.write(kubeconfig_content)
            tmp.flush()
            await config.load_kube_config(config_file=tmp.name)

        configuration = Configuration.get_default_copy()
        configuration.verify_ssl = False
        configuration.ssl_ca_cert = None
        return ApiClient(configuration=configuration)

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
                    _LITMUS_NS,
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
            for cex_manifest in build_chaos_exporter(namespace=_LITMUS_NS):
                await self._run_kubectl_apply_stdin(
                    kubeconfig_path=kc,
                    manifest_yaml=yaml.safe_dump(cex_manifest)
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
                        _LITMUS_NS,
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
        api_client = await self._build_api_client(kubeconfig_content)

        try:
            apps_v1 = client.AppsV1Api(api_client)
            dep = await asyncio.wait_for(
                apps_v1.read_namespaced_deployment(
                    name="chaos-operator-ce", namespace=_LITMUS_NS
                ),
                timeout=15.0
            )

            available = dep.status.available_replicas or 0

        except (ApiException, aiohttp.ClientError, asyncio.TimeoutError):
            return False
        else:
            return bool(available)
        finally:
            await api_client.close()

    async def setup_experiment_rbac(  # noqa: PLR0912
        self, kubeconfig_content: str, namespace: str
    ) -> None:
        api_client = await self._build_api_client(kubeconfig_content)
        try:
            v1 = client.CoreV1Api(api_client)
            rbac_v1 = client.RbacAuthorizationV1Api(api_client)
            custom = client.CustomObjectsApi(api_client)
            rbac_manifests = build_namespaced_litmuschaos_rbac(namespace=namespace)

            sa = rbac_manifests[0]
            try:
                await v1.create_namespaced_service_account(namespace=namespace, body=sa) # pyright: ignore[reportArgumentType]
            except ApiException as e:
                if e.status != _HTTP_CONFLICT:
                    raise

            # Apply Role (create or replace)
            role = rbac_manifests[1]
            try:
                await rbac_v1.create_namespaced_role(namespace=namespace, body=role) # pyright: ignore[reportArgumentType]
            except ApiException as e:
                if e.status == _HTTP_CONFLICT:
                    await rbac_v1.replace_namespaced_role(
                        name=role["metadata"]["name"], namespace=namespace, body=role # pyright: ignore[reportArgumentType]
                    )
                else:
                    raise

            # Apply RoleBinding (create or replace)
            role_binding = rbac_manifests[2]
            try:
                await rbac_v1.create_namespaced_role_binding(
                    namespace=namespace, body=role_binding # pyright: ignore[reportArgumentType]
                )
            except ApiException as e:
                if e.status == _HTTP_CONFLICT:
                    await rbac_v1.replace_namespaced_role_binding(
                        name=role_binding["metadata"]["name"],
                        namespace=namespace,
                        body=role_binding, # pyright: ignore[reportArgumentType]
                    )
                else:
                    raise

            # Apply ChaosExperiment templates (create or patch)
            for exp_type in EXPERIMENT_TEMPLATES:
                body = build_chaos_experiment(
                    experiment_type=exp_type,
                    namespace=namespace,
                    litmus_image=self._litmus_runner_image,
                )
                try:
                    await custom.create_namespaced_custom_object(
                        namespace=namespace,
                        body=body,
                        group=_LITMUS_CRD_GROUP,
                        version=_LITMUS_CRD_VERSION,
                        plural="chaosexperiments",
                    )
                except ApiException as e:
                    if e.status == _HTTP_CONFLICT:
                        existing = await custom.get_namespaced_custom_object(
                            group=_LITMUS_CRD_GROUP,
                            version=_LITMUS_CRD_VERSION,
                            namespace=namespace,
                            plural="chaosexperiments",
                            name=exp_type,
                        )
                        body["metadata"]["resourceVersion"] = existing["metadata"][
                            "resourceVersion"
                        ]
                        await custom.replace_namespaced_custom_object(
                            group=_LITMUS_CRD_GROUP,
                            version=_LITMUS_CRD_VERSION,
                            namespace=namespace,
                            plural="chaosexperiments",
                            name=exp_type,
                            body=body,
                        )
                    else:
                        raise

            logger.info("Litmus RBAC + experiment templates ready in ns=%s", namespace)
        finally:
            await api_client.close()

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
        body = build_chaos_engine(
            app_label=app_label,
            duration=duration,
            engine_name=engine_name,
            experiment_type=experiment_type,
            namespace=namespace,
            configuration=configuration,
            probes=probes,
        )

        api_client = await self._build_api_client(kubeconfig_content)
        try:
            custom = client.CustomObjectsApi(api_client)

            result: dict[str, Any] = await custom.create_namespaced_custom_object(
                namespace=namespace,
                body=body,
                group=_LITMUS_CRD_GROUP,
                version=_LITMUS_CRD_VERSION,
                plural="chaosengines",
            )
            logger.info(
                "ChaosEngine %s created in ns=%s with %d probe(s)",
                engine_name,
                namespace,
                len(probes) if probes else 0,
            )
            return result
        finally:
            await api_client.close()

    async def poll_experiment_result(
        self,
        kubeconfig_content: str,
        namespace: str,
        engine_name: str,
        experiment_type: str,
        timeout: int,  # noqa: ASYNC109
    ) -> dict[str, Any]:
        result_name = f"{engine_name}-{experiment_type}"
        api_client = await self._build_api_client(kubeconfig_content)

        try:
            custom = client.CustomObjectsApi(api_client)

            elapsed = 0
            while elapsed < timeout:
                try:
                    result: dict[str, Any] = await custom.get_namespaced_custom_object(
                        namespace=namespace,
                        group=_LITMUS_CRD_GROUP,
                        version=_LITMUS_CRD_VERSION,
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
                    if e.status != _HTTP_NOT_FOUND:
                        raise

                await asyncio.sleep(_POLL_INTERVAL)
                elapsed += _POLL_INTERVAL

            msg = f"Experiment {result_name} did not complete within {timeout}s"
            raise LitmusChaosExperimentError(msg)

        finally:
            await api_client.close()

    async def delete_experiment(
        self,
        kubeconfig_content: str,
        namespace: str,
        engine_name: str,
    ) -> None:
        api_client = await self._build_api_client(kubeconfig_content)
        try:
            custom = client.CustomObjectsApi(api_client)
            await custom.delete_namespaced_custom_object(
                group=_LITMUS_CRD_GROUP,
                version=_LITMUS_CRD_VERSION,
                namespace=namespace,
                plural="chaosengines",
                name=engine_name,
            )
            logger.info("ChaosEngine %s deleted from ns=%s", engine_name, namespace)

        except ApiException as e:
            if e.status != _HTTP_NOT_FOUND:
                raise
            logger.warning("ChaosEngine %s already gone", engine_name)

        finally:
            await api_client.close()

    async def ensure_cluster_ready(
        self,
        kubeconfig_content: str,
        namespace: str,
        target_label: str,
        timeout_s: int,
        interval_s: int,
        require_no_active_engine: bool = True
    ) -> None:
        # check all nodes is ready, all pods matching target_label in namespace is ready
        
        deadline = asyncio.get_event_loop().time() + timeout_s
        last_reason = "unknown"
        
        while asyncio.get_event_loop().time() < deadline:
            api_client = await self._build_api_client(kubeconfig_content)
            
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
                
                if not_ready_nodes:
                    last_reason = f"pods not ready: {not_ready_pods}"
                    await asyncio.sleep(interval_s)
                    continue
                
                
                # no active chaos engine
                if require_no_active_engine:
                    custom = client.CustomObjectsApi(api_client)
                    engines = await asyncio.wait_for(
                        custom.get_namespaced_custom_object(
                            group=_LITMUS_CRD_GROUP,
                            version=_LITMUS_CRD_VERSION,
                            namespace=namespace,
                            plural="chaosengines"
                        ), # type: ignore
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

            except (ApiException, aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_reason = f"API error: {e}"
                await asyncio.sleep(interval_s)
            finally:
                await api_client.close()
                
        raise ClusterNotReadyError(last_reason, timeout_s)