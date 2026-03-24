"""Application configuration settings."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Application
    ENV: str = "development"
    PROJECT_NAME: str = "chaos-platform"
    VERSION: str = "1.0.0"
    LOG_LEVEL: str = "INFO"

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
    KIND_BINARY: str = "/usr/local/bin/kind"
    KUBECTL_BINARY: str = "/usr/local/bin/kubectl"
    KUBECONFIG_DIR: str = "/tmp/kubeconfigs"  # noqa: S108
    KIND_PORT_RANGE_START: int = 30000
    KIND_PORT_RANGE_END: int = 32767
    KIND_PORTS_PER_BLOCK: int = 20
    CLUSTER_DEFAULT_TTL_DAYS: int = 7
    PRIVATE_REGISTRY_URL: str | None = None

    # Service Mesh / Linkerd
    LINKERD_BINARY: str = "/usr/local/bin/linkerd"
    GATEWAY_API_VERSION: str = "v1.4.0"
    LINKERD_INJECT_NAMESPACES: list[str] = ["default"]

    # Monitoring stack images
    VM_IMAGE: str = "victoriametrics/victoria-metrics:v1.108.1"
    KSM_IMAGE: str = "registry.k8s.io/kube-state-metrics/kube-state-metrics:v2.14.0"
    VM_NODEPORT: int = 30090

    # Deployment / Git settings
    GIT_CLONE_DIR: str = "/tmp/git-clones"  # noqa: S108

    # LitmusChaos
    LITMUS_VERSION: str = "3.9.0"

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

    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
