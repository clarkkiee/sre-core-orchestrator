from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml
from jinja2 import Environment, FileSystemLoader, StrictUndefined

logger = logging.getLogger(__name__)


def _to_yaml_filter(value: Any, indent: int = 0) -> str:
    dumped = yaml.dump(value, default_flow_style=False).rstrip()
    if indent:
        lines = dumped.splitlines()
        return "\n".join(
            (" " * indent + line) if i > 0 else line
            for i, line in enumerate(lines)
        )
    return dumped

class TemplateRenderer:
    def __init__(self, template_dir: Path) -> None:
        self._env = Environment(
            loader=FileSystemLoader(str(template_dir)),
            autoescape=False,
            trim_blocks=True,
            lstrip_blocks=True,
            undefined=StrictUndefined
        )
        self._env.filters["to_yaml"] = _to_yaml_filter
        
    def render_to_dicts(self, template_name: str, **ctx: Any) -> list[dict[str, Any]]:
        rendered = self._env.get_template(template_name).render(**ctx)
        docs = list(yaml.safe_load_all(rendered))
        result = [d for d in docs if d is not None]
        logger.debug(
            "Rendered template %r -> %d document(s)", template_name, len(result)
        )
        
        return result