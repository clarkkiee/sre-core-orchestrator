import asyncio
import logging
import os
import tempfile
from typing import Any

from kubernetes_asyncio import client, config
from kubernetes_asyncio.client import ApiClient, ApiException, Configuration

from app.infrastructure.chaos.exceptions import (
    LitmusChaosExperimentError,
    LitmusCommandError,
    LitmusDeployError,
)
from app.infrastructure.chaos.manifests import (
    EXPERIMENT_TEMPLATES,
    build_chaos_engine,
    build_chaos_experiment,
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
    ) -> None:
        self._kubectl_binary = kubectl_binary
        self._litmus_version = litmus_version

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
            dep = await apps_v1.read_namespaced_deployment(
                name="chaos-operator-ce", namespace=_LITMUS_NS
            )

            available = dep.status.available_replicas or 0

        except ApiException:
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
                await v1.create_namespaced_service_account(namespace=namespace, body=sa)
            except ApiException as e:
                if e.status != _HTTP_CONFLICT:
                    raise

            # Apply Role (create or replace)
            role = rbac_manifests[1]
            try:
                await rbac_v1.create_namespaced_role(namespace=namespace, body=role)
            except ApiException as e:
                if e.status == _HTTP_CONFLICT:
                    await rbac_v1.replace_namespaced_role(
                        name=role["metadata"]["name"], namespace=namespace, body=role
                    )
                else:
                    raise

            # Apply RoleBinding (create or replace)
            role_binding = rbac_manifests[2]
            try:
                await rbac_v1.create_namespaced_role_binding(
                    namespace=namespace, body=role_binding
                )
            except ApiException as e:
                if e.status == _HTTP_CONFLICT:
                    await rbac_v1.replace_namespaced_role_binding(
                        name=role_binding["metadata"]["name"],
                        namespace=namespace,
                        body=role_binding,
                    )
                else:
                    raise

            # Apply ChaosExperiment templates (create or patch)
            for exp_type in EXPERIMENT_TEMPLATES:
                body = build_chaos_experiment(
                    experiment_type=exp_type, namespace=namespace
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
    ) -> dict[str, Any]:
        body = build_chaos_engine(
            app_label=app_label,
            duration=duration,
            engine_name=engine_name,
            experiment_type=experiment_type,
            namespace=namespace,
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
            logger.info("ChaosEngine %s created in ns=%s", engine_name, namespace)
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
                        logger.info("ChaosResult %s verdict=%s", result_name, verdict)
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
