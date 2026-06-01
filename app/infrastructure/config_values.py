from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from app.infrastructure.template_renderer import TemplateRenderer
from app.utils.config import settings


def _config_dir() -> Path:
    return Path(settings.CONFIG_DIR)

@lru_cache(maxsize=1)
def load_values() -> dict[str, Any]:
    path = _config_dir() / "values" / "default.yaml"
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

@lru_cache(maxsize=1)
def load_cluster_profile() -> dict[str, Any]:
    path = _config_dir() / "cluster-profile.yaml"
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

@lru_cache(maxsize=1)
def get_renderer() -> TemplateRenderer:
    return TemplateRenderer(_config_dir() / "templates")
