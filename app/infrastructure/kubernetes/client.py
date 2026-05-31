import tempfile
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

from kubernetes_asyncio import config
from kubernetes_asyncio.client import ApiClient, Configuration

async def build_api_client(kubeconfig_content: str) -> ApiClient:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=True) as tmp:
        tmp.write(kubeconfig_content)
        tmp.flush()
        await config.load_kube_config(config_file=tmp.name)
    
    configuration = Configuration.get_default_copy()
    configuration.verify_ssl = False
    configuration.ssl_ca_cert = None
    return ApiClient(configuration=configuration)


@asynccontextmanager
async def k8s_client(kubeconfig_content: str) -> AsyncIterator[ApiClient]:
    api = await build_api_client(kubeconfig_content=kubeconfig_content)
    try:
        yield api
    finally:
        await api.close()