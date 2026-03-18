"""Kind cluster config builder with dynamic port-block allocation."""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Any

import yaml

from app.infrastructure.kind.exceptions import PortExhaustionError

logger = logging.getLogger(__name__)

PORT_RANGE_START = 30000
PORT_RANGE_END = 32767
PORTS_PER_BLOCK = 20

# Ports required per cluster (patched into the Kind config)
REQUIRED_PORTS = [
    # (container_port, protocol, listen_address)
    (80, "TCP", None),
    (443, "TCP", None),
    (30080, "TCP", "0.0.0.0"),  # noqa: S104
    (30443, "TCP", "0.0.0.0"),  # noqa: S104
    (30090, "TCP", "0.0.0.0"),  # noqa: S104  # VictoriaMetrics
]


class KindConfigBuilder:
    """Builds a Kind cluster config YAML with dynamically allocated ports."""

    def __init__(
        self,
        port_range_start: int = PORT_RANGE_START,
        port_range_end: int = PORT_RANGE_END,
        ports_per_block: int = PORTS_PER_BLOCK,
    ) -> None:
        self.port_range_start = port_range_start
        self.port_range_end = port_range_end
        self.ports_per_block = ports_per_block
        self._total_ports = port_range_end - port_range_start + 1
        self._total_blocks = self._total_ports // ports_per_block

    def compute_block_index(self, cluster_name: str) -> int:
        hash_val = int(
            hashlib.sha256(cluster_name.encode("utf-8")).hexdigest(),
            16,
        )
        return hash_val % self._total_blocks

    def find_available_block(
        self,
        cluster_name: str,
        occupied_blocks: set[int],
    ) -> int:
        preferred = self.compute_block_index(cluster_name)
        for offset in range(self._total_blocks):
            candidate = (preferred + offset) % self._total_blocks
            if candidate not in occupied_blocks:
                return candidate
        raise PortExhaustionError

    def get_port_block(self, block_index: int) -> list[int]:
        start = self.port_range_start + (block_index * self.ports_per_block)
        return list(range(start, start + self.ports_per_block))

    def allocate_ports(
        self,
        block_index: int,
    ) -> dict[str, Any]:
        """Allocate ports from a block. Returns a structured ports dict."""
        ports = self.get_port_block(block_index)
        # Port 0 → apiServerPort, ports 1..N → extraPortMappings
        api_server_port = ports[0]
        mapping_ports = ports[1 : 1 + len(REQUIRED_PORTS)]

        extra_port_mappings: list[dict[str, Any]] = []
        for port, (container_port, protocol, listen_addr) in zip(
            mapping_ports,
            REQUIRED_PORTS,
            strict=False,
        ):
            mapping: dict[str, Any] = {
                "containerPort": container_port,
                "hostPort": port,
                "protocol": protocol,
            }
            if listen_addr is not None:
                mapping["listenAddress"] = listen_addr
            extra_port_mappings.append(mapping)

        return {
            "block_index": block_index,
            "api_server_port": api_server_port,
            "extra_port_mappings": extra_port_mappings,
        }

    def build_config(
        self,
        cluster_name: str,
        ports_data: dict[str, Any],
        worker_count: int = 2,
        registry_url: str | None = None,
    ) -> dict[str, Any]:
        """Build a complete Kind cluster config dict."""
        # Control-plane node
        control_plane: dict[str, Any] = {
            "role": "control-plane",
            "extraPortMappings": ports_data["extra_port_mappings"],
            "kubeadmConfigPatches": [
                (
                    "kind: InitConfiguration\n"
                    "nodeRegistration:\n"
                    "  kubeletExtraArgs:\n"
                    '    node-labels: "ingress-ready=true"'
                ),
                (
                    "kind: JoinConfiguration\n"
                    "nodeRegistration:\n"
                    "  kubeletExtraArgs:\n"
                    '    node-labels: "master=true"'
                ),
            ],
        }

        # Worker nodes
        workers: list[dict[str, Any]] = []
        for _ in range(worker_count):
            workers.append(
                {
                    "role": "worker",
                    "kubeadmConfigPatches": [
                        (
                            "kind: JoinConfiguration\n"
                            "nodeRegistration:\n"
                            "  kubeletExtraArgs:\n"
                            '    node-labels: "worker=true"'
                        ),
                    ],
                }
            )

        config: dict[str, Any] = {
            "kind": "Cluster",
            "apiVersion": "kind.x-k8s.io/v1alpha4",
            "name": cluster_name,
            "networking": {
                "apiServerPort": ports_data["api_server_port"],
            },
            "nodes": [control_plane, *workers],
        }

        # Optional containerd registry mirror
        if registry_url:
            config["containerdConfigPatches"] = [
                (
                    f'[plugins."io.containerd.grpc.v1.cri"'
                    f'.registry.mirrors."{registry_url}"]\n'
                    f'  endpoint = ["http://{registry_url}"]\n'
                    f'  [plugins."io.containerd.grpc.v1.cri"'
                    f'.registry.configs."{registry_url}".tls]\n'
                    f"  insecure_skip_verify = true"
                ),
            ]

        return config

    def write_config(self, config: dict[str, Any], output_path: Path) -> Path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(config, f, sort_keys=False)
        logger.info("Kind config written to %s", output_path)
        return output_path
