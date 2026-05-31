"""Application configuration settings."""

from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path

class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=True,
        extra="ignore"
    )

    # Application
    ENV: str = "development"
    PROJECT_NAME: str = "chaos-platform"
    VERSION: str = "1.0.0"
    LOG_LEVEL: str = "INFO"
    CONFIG_DIR: str = str(Path(__file__).resolve().parents[2]/"config")

    # Database
    POSTGRES_HOST: str = "db"
    POSTGRES_PORT: int = 5432
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"  # noqa: S105
    POSTGRES_DB: str = "chaos_platform"
    POSTGRES_POOL_SIZE: int = 5
    POSTGRES_MAX_OVERFLOW: int = 10
    POSTGRES_POOL_TIMEOUT: int = 30

    # RabbitMQ
    RABBITMQ_HOST: str = "rabbitmq"
    RABBITMQ_PORT: int = 5672
    RABBITMQ_USER: str = "guest"
    RABBITMQ_PASSWORD: str = "guest"  # noqa: S105

    # JWT
    JWT_ALGORITHM: str = "HS256"
    JWT_ACCESS_TOKEN_EXPIRES_MINUTES: int = 120
    JWT_REFRESH_TOKEN_EXPIRES_DAYS: int = 7
    JWT_SECRET_KEY: str = "secret_key"  # noqa: S105

    # API
    ALLOWED_ORIGINS: str = "http://localhost:3000,http://localhost:8000"

    # Docker / Kind cluster provisioning
    DOCKER_HOST: str | None = None  # For K8s DinD sidecar: "tcp://localhost:2375"
    KUBECTL_BINARY: str = "/usr/local/bin/kubectl"
    KUBECONFIG_DIR: str = "/tmp/kubeconfigs"  # noqa: S108
    CLUSTER_DEFAULT_TTL_DAYS: int = 7
    PRIVATE_REGISTRY_URL: str | None = None
    CLUSTER_PROVIDER: str = "multipass_k3s"
    # Multipass + k3s settings (used when CLUSTER_PROVIDER=multipass_k3s)
    MULTIPASS_BINARY: str = "/snap/bin/multipass"
    K3S_DISABLE_TRAEFIK: bool = True

    # Provisioning Agent settings
    # When MULTIPASS_USE_AGENT=true the orchestrator delegates to a Go agent
    # on the host instead of running multipass commands over SSH.
    MULTIPASS_USE_AGENT: bool = True
    AGENT_HOST: str = "172.17.0.1"  # Falls back to SSH_HOST if empty
    AGENT_PORT: int = 9090
    AGENT_API_TOKEN: str = ""
    AGENT_BINARY_PATH: str = "/app/agent/orchestrator-agent"
    AGENT_POLL_INTERVAL: float = 3.0
    AGENT_POLL_TIMEOUT: int = 1800
    AGENT_BOOTSTRAP_TIMEOUT: int = 30

    # SSH settings for remote Multipass execution
    # When MULTIPASS_USE_SSH=true the orchestrator connects to SSH_HOST
    # and runs multipass commands there (required when running in Docker).
    MULTIPASS_USE_SSH: bool = True
    SSH_HOST: str = "127.0.0.1"
    SSH_PORT: int = 22
    SSH_USER: str = ""
    SSH_KEY_PATH: str | None = None
    SSH_PASSWORD: str | None = None

    # Service Mesh / Linkerd
    LINKERD_BINARY: str = "/usr/local/bin/linkerd"
    GATEWAY_API_VERSION: str = "v1.4.0"
    LINKERD_INJECT_NAMESPACES: list[str] = ["default"]

    # Deployment / Git settings
    GIT_CLONE_DIR: str = "/tmp/git-clones"  # noqa: S108

    # Cluster Readiness
    CLUSTER_READY_TIMEOUT_S: int = 60
    CLUSTER_READY_INTERVAL_S: int = 5
    CLUSTER_READY_REQUIRE_NO_ACTIVE_ENGINE: bool = True

    PROBE_DEFAULT_SUCCESS_RATE_SLO: float = 0.95
    PROBE_DEFAULT_DEGRADED_SUCCESS_RATE_SLO: float = 0.80
    PROBE_DEFAULT_P99_RECOVERY_RATIO: float = 1.2
    PROBE_DEFAULT_LIVENESS_TIMEOUT_S: int = 3
    PROBE_DEFAULT_LIVENESS_POLL_S: int = 5
    PROBE_DEFAULT_RECOVERY_INITIAL_DELAY_S: int = 30
    PROBE_DEFAULT_RECOVERY_RETRY: int = 9
    PROBE_DEFAULT_RECOVERY_INTERVAL_S: int = 10
    PROBE_DEFAULT_MEMORY_RESTART_MAX: int = 1
    BASELINE_METRICS_ENABLED: bool = True
    PROBE_LATENCY_TOLERANCE_FACTOR: float = 1.5
    PROBE_SUCCESS_TOLERANCE_FACTOR: float = 0.95

    # Probe target endpoint defaults (deployment can override via probe_thresholds).
    PROBE_DEFAULT_TARGET_PORT: int = 80
    PROBE_DEFAULT_TARGET_HEALTH_PATH: str = "/healthz"

    # Absolute latency thresholds (ms) — used by promProbe SOT/EOT comparators.
    PROBE_DEFAULT_P95_BASELINE_THRESHOLD_MS: int = 800
    PROBE_DEFAULT_P95_RECOVERY_THRESHOLD_MS: int = 1200
    PROBE_DEFAULT_P99_BASELINE_THRESHOLD_MS: int = 1000
    PROBE_DEFAULT_P99_RECOVERY_THRESHOLD_MS: int = 1500

    # Linkerd PromQL rate window for success-rate / latency probes.
    PROBE_DEFAULT_LINKERD_WINDOW: str = "120s"
    PROBE_DEFAULT_RECOVERY_PROBE_WINDOW: str = "30s"

    # Container images for cmdProbe source-mode.
    PROBE_DEFAULT_CMD_PROBE_IMAGE: str = "curlimages/curl:8.6.0"
    PROBE_DEFAULT_TCP_CMD_PROBE_IMAGE: str = "nicolaka/netshoot:latest"
    PROBE_DEFAULT_KUBECTL_PROBE_IMAGE: str = "bitnami/kubectl:1.28"

    @property
    def DATABASE_URL(self) -> str:  # noqa: N802
        """Construct async database URL."""
        return (
            f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    @property
    def DATABASE_URL_SYNC(self) -> str:  # noqa: N802
        """Construct sync database URL (for Alembic)."""
        return (
            f"postgresql+psycopg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    @property
    def CELERY_BROKER_URL(self) -> str:  # noqa: N802
        """Construct Celery broker URL."""
        return (
            f"amqp://{self.RABBITMQ_USER}:{self.RABBITMQ_PASSWORD}"
            f"@{self.RABBITMQ_HOST}:{self.RABBITMQ_PORT}//"
        )


settings = Settings()
