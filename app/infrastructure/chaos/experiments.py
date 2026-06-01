from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from app.utils.config import settings

_RESERVED_CONFIG_KEYS = {"probes"}

@lru_cache
def _registry() -> dict[str, dict[str, Any]]:
    exp_dir = Path(settings.CONFIG_DIR) / "experiments"
    registry: dict[str, dict[str, Any]] = {}
    for path in sorted(exp_dir.glob("*.yaml")):
        with path.open(encoding="utf-8") as f:
            registry[path.stem] = yaml.safe_load(f) or {}
    return registry

def experiment_names() -> list[str]:
    return list(_registry().keys())

def get_experiment(name: str) -> dict[str, Any]:
    return _registry()[name]

def needs_runtime_socket(name: str) -> bool:
    return bool(get_experiment(name).get("needs_runtime_socket", False))

def build_experiment_env_vars(name: str) -> list[dict[str, str]]:
    env = get_experiment(name).get("env", {})
    return [{"name": k, "value": str(v)} for k, v in env.items()]

def build_engine_env_vars(
    name: str,
    duration: int,
    configuration: dict[str, Any] | None = None,
) -> list[dict[str, str]]:
    env = dict(get_experiment(name).get("env", {}))
    env["TOTAL_CHAOS_DURATION"] = str(duration)
    if configuration:
        for key, value in configuration.items():
            if key in _RESERVED_CONFIG_KEYS:
                continue
            env[key] = str(value).lower() if isinstance(value, bool) else str(value)
    return [{"name": k, "value": v} for k, v in env.items()]
