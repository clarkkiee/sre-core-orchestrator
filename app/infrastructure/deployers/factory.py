"""Factory for creating deployer instances based on strategy."""

from typing import ClassVar

from app.infrastructure.deployers.base import BaseDeployer
from app.infrastructure.deployers.helm import HelmDeployer
from app.infrastructure.deployers.kustomize import KustomizeDeployer
from app.infrastructure.deployers.raw import RawDeployer
from app.infrastructure.deployers.skaffold import SkaffoldDeployer
from app.schemas.platform_config import DeployStrategy, PlatformDeploy


class DeployerFactory:
    """Returns the correct deployer implementation based on strategy."""

    _deployer_map: ClassVar[dict[DeployStrategy, type[BaseDeployer]]] = {
        DeployStrategy.RAW: RawDeployer,
        DeployStrategy.HELM: HelmDeployer,
        DeployStrategy.SKAFFOLD: SkaffoldDeployer,
        DeployStrategy.KUSTOMIZE: KustomizeDeployer,
    }

    @classmethod
    def create(
        cls,
        deploy_config: PlatformDeploy,
        kubeconfig_path: str,
        repo_path: str,
        namespace: str,
    ) -> BaseDeployer:
        deployer_class = cls._deployer_map.get(deploy_config.strategy)
        if deployer_class is None:
            msg = f"Unsupported deploy strategy: {deploy_config.strategy}"
            raise ValueError(msg)

        return deployer_class(
            kubeconfig_path=kubeconfig_path,
            repo_path=repo_path,
            namespace=namespace,
            strategy_config=deploy_config.get_strategy_config(),
        )
