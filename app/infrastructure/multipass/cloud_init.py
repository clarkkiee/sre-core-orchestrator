"""Cloud-init config generation for Multipass VMs."""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path

import yaml

from app.infrastructure.multipass.exceptions import CloudInitError

logger = logging.getLogger(__name__)

# Packages needed for k3s and LitmusChaos network chaos experiments
_BASE_PACKAGES = [
    "curl",
    "iproute2",       # tc (traffic control) for network chaos
    "iptables",        # LitmusChaos network-level rules
    "conntrack",       # connection tracking for kube-proxy
    "open-iscsi",      # optional: for persistent volumes
]

# Kernel modules required for LitmusChaos network chaos
_CHAOS_KERNEL_MODULES = [
    "sch_netem",       # network emulation (delay, loss, corruption)
    "sch_tbf",         # token bucket filter (rate limiting)
    "sch_sfq",         # stochastic fairness queueing
]


class CloudInitBuilder:
    """Generates cloud-init YAML configs for Multipass VMs."""

    def build_server_config(
        self,
        *,
        extra_packages: list[str] | None = None,
    ) -> str:
        """Generate cloud-init for the k3s server node."""
        return self._build(extra_packages=extra_packages)

    def build_agent_config(
        self,
        *,
        extra_packages: list[str] | None = None,
    ) -> str:
        """Generate cloud-init for k3s agent (worker) nodes."""
        return self._build(extra_packages=extra_packages)

    def write_to_tempfile(self, content: str) -> str:
        """Write cloud-init YAML to a temp file. Returns the file path.

        The caller is responsible for cleanup. In practice the file is only
        needed until ``multipass launch`` reads it, so it can be deleted
        after the launch call returns.
        """
        tmp = tempfile.NamedTemporaryFile(
            mode="w",
            suffix="-cloud-init.yaml",
            delete=False,
            prefix="mp-",
        )
        try:
            tmp.write(content)
            tmp.flush()
            logger.info("Cloud-init written to %s", tmp.name)
            return tmp.name
        finally:
            tmp.close()

    def _build(
        self,
        *,
        extra_packages: list[str] | None = None,
    ) -> str:
        """Build a cloud-init YAML string."""
        packages = list(_BASE_PACKAGES)
        if extra_packages:
            packages.extend(extra_packages)

        # Load kernel modules at boot and immediately
        modprobe_cmds = [
            f"modprobe {mod}" for mod in _CHAOS_KERNEL_MODULES
        ]
        persist_cmds = [
            f'echo "{mod}" >> /etc/modules-load.d/chaos.conf'
            for mod in _CHAOS_KERNEL_MODULES
        ]

        config: dict = {
            "package_update": False,
            "packages": packages,
            "runcmd": [
                *modprobe_cmds,
                *persist_cmds,
            ],
        }

        try:
            return "#cloud-config\n" + yaml.safe_dump(config, sort_keys=False)
        except yaml.YAMLError as exc:
            msg = f"Failed to generate cloud-init YAML: {exc}"
            raise CloudInitError(msg) from exc
