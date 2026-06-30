from __future__ import annotations

from typing import TYPE_CHECKING

from app.infrastructure.providers.base import ClusterProvider

if TYPE_CHECKING:
    from app.repositories.cluster import ClusterRepository


def get_provider(
    provider_type: str,
    *,
    cluster_repo: ClusterRepository | None = None,
) -> ClusterProvider:
    if provider_type == "multipass_k3s":
        from app.utils.config import settings as _settings

        if _settings.MULTIPASS_USE_AGENT:
            from app.infrastructure.providers.multipass_agent_provider import (
                MultipassAgentProvider,
            )

            return MultipassAgentProvider()

        if _settings.MULTIPASS_USE_SSH:
            from app.infrastructure.providers.multipass_k3s_ssh_provider import (
                MultipassK3sSSHProvider,
            )

            return MultipassK3sSSHProvider()

        from app.infrastructure.providers.multipass_k3s_provider import (
            MultipassK3sProvider,
        )

        return MultipassK3sProvider()
    msg = f"Unknown cluster provider: {provider_type!r}"
    raise ValueError(msg)
