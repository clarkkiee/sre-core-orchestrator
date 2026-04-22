from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from string import Template
from typing import Any

import yaml

logger = logging.getLogger(__name__)

_ALLOWED_VALUE_TYPES = frozenset({"gauge", "counter", "histogram"})
_ALLOWED_SOURCES = frozenset({
    "blackbox_exporter",
    "kube_state_metrics",
    "linkerd_proxy",
    "kubelet_cadvisor"
})

class MetricCatalogError(Exception):
    """Raised when fails to load or validate metrics catalog"""

@dataclass(frozen=True)
class MetricDefinition:
    name: str
    source: str
    description: str
    promql_template: str
    unit: str
    value_type: str
    default_step: str = "5s"
    labels_to_keep: tuple[str, ...] = ()

    def render(self, params: dict[str, str]) -> str:
        """Render PromQL template"""
        try:
            return Template(self.promql_template).substitute(params)
        except KeyError as e:
            msg = (
                f"Metric '{self.name}' requires parameter {e} "
                f"(template: {self.promql_template})"
            )
            raise MetricCatalogError(msg) from e

class MetricCatalog:
    def __init__(self, definitions: dict[str, MetricDefinition]) -> None:
        self._defs = definitions

    @classmethod
    def load_from_dir(cls, path: Path | None = None) -> MetricCatalog:
        if path is None:
            path = Path(__file__).resolve().parent / "catalog_files"
        if not path.is_dir():
            msg = f"Catalog directory not found: {path}"
            raise MetricCatalogError(msg)

        definitions: dict[str, MetricDefinition] = {}
        yaml_files = sorted(path.glob("*.yaml")) + sorted(path.glob("*.yml"))

        if not yaml_files:
            msg = f"No YAML files found in path: {path}"
            raise MetricCatalogError(msg)

        for file_path in yaml_files:
            for raw in _read_yaml_entries(file_path):
                definition = _build_definition(raw, file_path)
                if definition.name in definitions:
                    existing = definitions[definition.name]
                    msg = (
                        f"Duplicate metric name '{definition.name}' "
                        f"(already defined for source '{existing.source}')"
                    )
                    raise MetricCatalogError(msg)
                definitions[definition.name] = definition

        logger.info(
            "Loaded %d metric definitions from %s",
            len(definitions), path,
        )

        return cls(definitions)

    def get(self, name: str) -> MetricDefinition:
        try:
            return self._defs[name]
        except KeyError as e:
            msg = f"Unknown metrics: {name}"
            raise MetricCatalogError(msg) from e

    def list_by_source(self, source: str) -> list[MetricDefinition]:
        return [d for d in self._defs.values() if d.source == source]

    def all(self) -> list[MetricDefinition]:
        return list(self._defs.values())

    def __contains__(self, name: str) -> bool:
        return name in self._defs

    def __len__(self) -> int:
        return len(self._defs)

# helpers
def _read_yaml_entries(file_path: Path) -> list[dict[str, Any]]:
    try:
        with file_path.open("r", encoding="utf-8") as f:
            doc = yaml.safe_load(f) or {}
    except yaml.YAMLError as e:
        msg = f"Invalid YAML in {file_path}: {e}"
        raise MetricCatalogError(msg) from e

    entries = doc.get("metrics")
    if not isinstance(entries, list):
        msg = f"{file_path}: top-level key 'metrics' must be a list"
        raise MetricCatalogError(msg)

    return entries

def _build_definition(raw: dict[str, Any], file_path: Path) -> MetricDefinition:
    required = {"name", "source", "promql_template", "unit", "value_type"}
    missing = [k for k in required if not raw.get(k)]
    if missing:
        msg = (
            f"{file_path}: metrics entry missing required fields {missing} "
            f"(entry): {raw}"
        )
        raise MetricCatalogError(msg)

    source = raw["source"]
    if source not in _ALLOWED_SOURCES:
        msg = (
            f"{file_path}: metric '{raw['name']}' has unknown source '{source}'"
            f"Allowed: {sorted(_ALLOWED_SOURCES)}"
        )
        raise MetricCatalogError(msg)

    value_type = raw["value_type"]
    if value_type not in _ALLOWED_VALUE_TYPES:
        msg = (
            f"{file_path}: metric '{raw['name']}' has unknown value_type '{value_type}'"
            f"Allowed: {sorted(_ALLOWED_VALUE_TYPES)}"
        )
        raise MetricCatalogError(msg)

    labels_to_keep = raw.get("labels_to_keep") or []
    if not isinstance(labels_to_keep, list) or not all(
        isinstance(x, str) for x in labels_to_keep
    ):
        msg = f"{file_path}: key 'labels_to_keep' must be a list of strings"
        raise MetricCatalogError(msg)

    return MetricDefinition(
        name=raw["name"],
        source=source,
        description=raw.get("description", ""),
        promql_template=raw["promql_template"],
        unit=raw["unit"],
        value_type=value_type,
        default_step=raw.get("default_step", "5s"),
        labels_to_keep=tuple(labels_to_keep),
    )
