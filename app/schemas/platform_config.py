"""Pydantic schemas for .platform.yaml convention file parsing."""

from enum import StrEnum

from pydantic import BaseModel, Field, model_validator


class DeployStrategy(StrEnum):
    RAW = "raw"
    HELM = "helm"
    SKAFFOLD = "skaffold"
    KUSTOMIZE = "kustomize"


# --- Strategy-specific config models ---


class RawConfig(BaseModel):
    path: str = Field(
        ...,
        description="Path to raw Kubernetes YAML manifests",
    )


class HelmConfig(BaseModel):
    chart_path: str = Field(
        ...,
        description="Path to Helm chart directory",
    )
    values_path: str | None = Field(
        None,
        description="Path to Helm values file",
    )
    release_name: str | None = Field(
        None,
        description="Helm release name (defaults to metadata.name)",
    )


class SkaffoldConfig(BaseModel):
    config_path: str = Field(
        default="./skaffold.yaml",
        description="Path to skaffold.yaml config",
    )
    profile: str | None = Field(
        default=None,
        description="Skaffold profile to activate",
    )
    default_repo: str = Field(
        default="",
        description="Default image repo for skaffold (empty string for local builds)",
    )


class KustomizeConfig(BaseModel):
    path: str = Field(
        ...,
        description="Path to kustomize directory",
    )


# --- Sections of .platform.yaml ---


class PlatformMetadata(BaseModel):
    name: str | None = None
    team: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)


class PlatformSource(BaseModel):
    branch: str = Field(
        default="main",
        description="Git branch to checkout",
    )


class PlatformDeploy(BaseModel):
    strategy: DeployStrategy
    raw: RawConfig | None = None
    helm: HelmConfig | None = None
    skaffold: SkaffoldConfig | None = None
    kustomize: KustomizeConfig | None = None

    @model_validator(mode="after")
    def validate_strategy_config_present(self) -> "PlatformDeploy":
        """Ensure the strategy-specific config block is provided and matches."""
        strategy_config_map = {
            DeployStrategy.RAW: self.raw,
            DeployStrategy.HELM: self.helm,
            DeployStrategy.SKAFFOLD: self.skaffold,
            DeployStrategy.KUSTOMIZE: self.kustomize,
        }
        config = strategy_config_map.get(self.strategy)
        if config is None:
            msg = (
                f"Strategy '{self.strategy}' requires a matching "
                f"'{self.strategy}' configuration block"
            )
            raise ValueError(msg)
        return self

    def get_strategy_config(
        self,
    ) -> RawConfig | HelmConfig | SkaffoldConfig | KustomizeConfig:
        """Return the active strategy configuration block."""
        mapping: dict[
            DeployStrategy,
            RawConfig | HelmConfig | SkaffoldConfig | KustomizeConfig | None,
        ] = {
            DeployStrategy.RAW: self.raw,
            DeployStrategy.HELM: self.helm,
            DeployStrategy.SKAFFOLD: self.skaffold,
            DeployStrategy.KUSTOMIZE: self.kustomize,
        }
        config = mapping[self.strategy]
        assert config is not None  # guaranteed by model_validator  # noqa: S101
        return config


class PlatformCluster(BaseModel):
    create_namespace: bool = Field(
        default=True,
        description="Whether to create the namespace if it does not exist",
    )


class PlatformConfig(BaseModel):
    """Root model for the .platform.yaml convention file."""

    apiVersion: str = Field(  # noqa: N815
        ...,
        pattern=r"^platform\.io/v\d+$",
        description="API version, e.g., platform.io/v1",
    )
    kind: str = Field(
        ...,
        description="Must be 'Application'",
    )
    metadata: PlatformMetadata = Field(default_factory=PlatformMetadata)
    source: PlatformSource
    deploy: PlatformDeploy
    cluster: PlatformCluster = Field(default_factory=PlatformCluster)

    @model_validator(mode="after")
    def validate_kind(self) -> "PlatformConfig":
        if self.kind != "Application":
            msg = f"kind must be 'Application', got '{self.kind}'"
            raise ValueError(msg)
        return self
