"""Litmus probe template registry and builder for ChaosEngine integration.

Threshold resolution order (highest priority last):
  1. Global PROBE_DEFAULT_* settings
  2. Deployment.probe_thresholds JSONB
  3. ChaosExperiment.configuration["probes"]["thresholds"]

Override modes (StartChaosExperimentRequest.configuration["probes"]):
  - "override": list[dict]   — full replacement of default templates (skip defaults)
  - "additional": list[dict] — append after defaults
  - "disable": list[str]     — remove probes by name from defaults
  - "thresholds": dict       — per-experiment threshold tweaks
"""

from __future__ import annotations

import copy
import logging
from string import Template
from typing import Any, TypedDict

from app.utils.config import Settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


class ProbeOverrides(TypedDict, total=False):
    """User-supplied overrides via experiment.configuration["probes"]."""

    override: list[dict[str, Any]]
    additional: list[dict[str, Any]]
    disable: list[str]
    thresholds: dict[str, Any]


# ---------------------------------------------------------------------------
# Threshold resolution
# ---------------------------------------------------------------------------


def _global_thresholds(settings: Settings) -> dict[str, Any]:
    """Threshold defaults sourced from PROBE_DEFAULT_* settings."""
    return {
        "target_port": settings.PROBE_DEFAULT_TARGET_PORT,
        "target_health_path": settings.PROBE_DEFAULT_TARGET_HEALTH_PATH,
        "success_rate_slo": settings.PROBE_DEFAULT_SUCCESS_RATE_SLO,
        "degraded_success_rate_slo": settings.PROBE_DEFAULT_DEGRADED_SUCCESS_RATE_SLO,
        "p99_baseline_threshold_ms": settings.PROBE_DEFAULT_P99_BASELINE_THRESHOLD_MS,
        "p99_recovery_threshold_ms": settings.PROBE_DEFAULT_P99_RECOVERY_THRESHOLD_MS,
        "liveness_timeout_s": settings.PROBE_DEFAULT_LIVENESS_TIMEOUT_S,
        "liveness_poll_s": settings.PROBE_DEFAULT_LIVENESS_POLL_S,
        "recovery_initial_delay_s": settings.PROBE_DEFAULT_RECOVERY_INITIAL_DELAY_S,
        "recovery_retry": settings.PROBE_DEFAULT_RECOVERY_RETRY,
        "recovery_interval_s": settings.PROBE_DEFAULT_RECOVERY_INTERVAL_S,
        "memory_restart_max": settings.PROBE_DEFAULT_MEMORY_RESTART_MAX,
        "linkerd_window": settings.PROBE_DEFAULT_LINKERD_WINDOW,
        "cmd_probe_image": settings.PROBE_DEFAULT_CMD_PROBE_IMAGE,
        "tcp_probe_image": settings.PROBE_DEFAULT_TCP_CMD_PROBE_IMAGE,
        "kubectl_probe_image": settings.PROBE_DEFAULT_KUBECTL_PROBE_IMAGE,
    }


def resolve_thresholds(
    *,
    settings: Settings,
    deployment_thresholds: dict[str, Any] | None = None,
    experiment_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Resolve threshold values via 3-layer override chain."""
    merged = _global_thresholds(settings)
    if deployment_thresholds:
        merged.update(deployment_thresholds)
    if experiment_overrides:
        merged.update(experiment_overrides)
    return merged


# ---------------------------------------------------------------------------
# Template rendering
# ---------------------------------------------------------------------------


def _extract_target_name(app_label: str) -> str:
    """Extract the value side of a label selector ("app=frontend" → "frontend").

    Falls back to the full string if no '=' present (defensive).
    """
    if "=" not in app_label:
        return app_label
    return app_label.split("=", 1)[1].strip()


def _render(value: Any, ctx: dict[str, Any]) -> Any:  # noqa: ANN401
    """Recursively substitute ${var} placeholders using string.Template.

    string.Template uses $-syntax which doesn't conflict with PromQL's `{}` chars.
    Non-string scalars and dict keys are left untouched.
    """
    if isinstance(value, str):
        try:
            return Template(value).substitute(ctx)
        except (KeyError, ValueError) as exc:
            logger.warning("Probe template substitution failed for %r: %s", value, exc)
            return value
    if isinstance(value, dict):
        return {k: _render(v, ctx) for k, v in value.items()}
    if isinstance(value, list):
        return [_render(item, ctx) for item in value]
    return value


def _coerce_probe_types(spec: dict[str, Any]) -> None:
    """Normalize rendered probe fields to the types expected by Litmus."""
    run_properties = spec.get("runProperties")
    if not isinstance(run_properties, dict):
        return

    retry = run_properties.get("retry")
    if isinstance(retry, str) and retry.isdigit():
        run_properties["retry"] = int(retry)


def _tcp_connect_command() -> str:
    return (
        "nc -z -w 3 ${target_clusterip} "
        "${target_port} && echo 'OK' || echo 'FAIL'"
    )


def _tcp_unreachable_command() -> str:
    return (
        "nc -z -w 3 ${target_clusterip} "
        "${target_port} && echo 'REACHABLE' || echo 'TIMEOUT'"
    )


# ---------------------------------------------------------------------------
# Probe template fragments — shared building blocks
# ---------------------------------------------------------------------------

# SOT guard: service must respond 200 before chaos. stopOnFailure aborts engine.
_SOT_HTTP_BASELINE: dict[str, Any] = {
    "name": "${target_name}-baseline-up",
    "type": "httpProbe",
    "mode": "SOT",
    "runProperties": {
        "probeTimeout": "5s",
        "interval": "2s",
        "retry": 1,
        "stopOnFailure": True,
    },
    "httpProbe/inputs": {
        "url": "http://${target_name}.${namespace}.svc.cluster.local:${target_port}${target_health_path}",
        "method": {
            "get": {
                "criteria": "==",
                "responseCode": "200"
            }
        },
        "insecureSkipVerify": True,
    },
}

# SOT tcp connectivity check: service must accept a TCP connection before chaos.
_SOT_TCP_BASELINE: dict[str, Any] = {
    "name": "${target_name}-baseline-up",
    "type": "cmdProbe",
    "mode": "SOT",
    "runProperties": {
        "probeTimeout": "5s",
        "interval": "2s",
        "retry": 1,
        "stopOnFailure": True,
    },
    "cmdProbe/inputs": {
        "command": _tcp_connect_command(),
        "comparator": {
            "type": "string",
            "criteria": "contains",
            "value": "OK",
        },
        "source": {
            "image": "${tcp_probe_image}",
            "hostNetwork": True,
        },
    },
}

# Continuous httpProbe: liveness signal during chaos. Drives probeSuccessPercentage.
_CONTINUOUS_HTTP_LIVENESS: dict[str, Any] = {
    "name": "${target_name}-availability",
    "type": "httpProbe",
    "mode": "Continuous",
    "runProperties": {
        "probeTimeout": "${liveness_timeout_s}s",
        "interval": "${liveness_poll_s}s",
        "retry": 0,
        "probePollingInterval": "${liveness_poll_s}s",
        "stopOnFailure": False,
    },
    "httpProbe/inputs": {
        "url": "http://${target_name}.${namespace}.svc.cluster.local:${target_port}${target_health_path}",
        "insecureSkipVerify": True,
        "method": {"get": {"criteria": "==", "responseCode": "200"}},
    },
}

# Continuous tcp connectivity check for non-HTTP services.
_CONTINUOUS_TCP_LIVENESS: dict[str, Any] = {
    "name": "${target_name}-availability",
    "type": "cmdProbe",
    "mode": "Continuous",
    "runProperties": {
        "probeTimeout": "${liveness_timeout_s}s",
        "interval": "${liveness_poll_s}s",
        "retry": 0,
        "probePollingInterval": "${liveness_poll_s}s",
        "stopOnFailure": False,
    },
    "cmdProbe/inputs": {
        "command": _tcp_connect_command(),
        "comparator": {
            "type": "string",
            "criteria": "contains",
            "value": "OK",
        },
        "source": {
            "image": "${tcp_probe_image}",
            "hostNetwork": True,
        },
    },
}

# EOT k8sProbe: deployment must be present (with at least one ready replica via labelSelector).
_EOT_K8S_DEPLOYMENT_PRESENT: dict[str, Any] = {
    "name": "${target_name}-replica-restored",
    "type": "k8sProbe",
    "mode": "EOT",
    "runProperties": {
        "probeTimeout": "10s",
        "interval": "${recovery_interval_s}s",
        "retry": "${recovery_retry}",
        "initialDelay": "${recovery_initial_delay_s}s",
        "stopOnFailure": False,
    },
    "k8sProbe/inputs": {
        "group": "apps",
        "version": "v1",
        "resource": "deployments",
        "namespace": "${namespace}",
        "labelSelector": "${target_label}",
        "operation": "present",
    },
}

# SOT promProbe: baseline P99 must be below absolute threshold (sanity guard).
_SOT_PROM_P99_BASELINE: dict[str, Any] = {
    "name": "${target_name}-baseline-p99",
    "type": "promProbe",
    "mode": "SOT",
    "runProperties": {
        "probeTimeout": "5s",
        "interval": "5s",
        "retry": 1,
        "stopOnFailure": True,
    },
    "promProbe/inputs": {
        "endpoint": "${prom_url}",
        "query": (
            'histogram_quantile(0.99, sum(rate('
            'response_latency_ms_bucket{deployment=\\"${target_name}\\",direction=\\"inbound\\"}'
            '[${linkerd_window}])) by (le))'
        ),
        "comparator": {
            "type": "float",
            "criteria": "<=",
            "value": "${p99_baseline_threshold_ms}",
        },
    },
}

# Continuous promProbe: success rate ≥ SLO during chaos.
_CONTINUOUS_PROM_SUCCESS_RATE: dict[str, Any] = {
    "name": "${target_name}-slo-success-rate",
    "type": "promProbe",
    "mode": "Continuous",
    "runProperties": {
        "probeTimeout": "5s",
        "interval": "10s",
        "probePollingInterval": "10s",
        "retry": 1,
        "stopOnFailure": False,
    },
    "promProbe/inputs": {
        "endpoint": "${prom_url}",
        "query": (
            'sum(rate(response_total{deployment=\\"${target_name}\\",direction=\\"inbound\\",'
            'classification=\\"success\\"}[${linkerd_window}])) '
            "/ "
            'sum(rate(response_total{deployment=\\"${target_name}\\",direction=\\"inbound\\"}'
            "[${linkerd_window}]))"
        ),
        "comparator": {
            "type": "float",
            "criteria": ">=",
            "value": "${success_rate_slo}",
        },
    },
}

# Continuous promProbe with looser SLO (network-latency / network-loss scenarios).
_CONTINUOUS_PROM_DEGRADED_SUCCESS_RATE: dict[str, Any] = {
    "name": "${target_name}-slo-success-rate-degraded",
    "type": "promProbe",
    "mode": "Continuous",
    "runProperties": {
        "probeTimeout": "5s",
        "interval": "10s",
        "probePollingInterval": "10s",
        "retry": 1,
        "stopOnFailure": False,
    },
    "promProbe/inputs": {
        "endpoint": "${prom_url}",
        "query": (
            'sum(rate(response_total{deployment=\\"${target_name}\\",direction=\\"inbound\\",'
            'classification=\\"success\\"}[${linkerd_window}])) '
            "/ "
            'sum(rate(response_total{deployment=\\"${target_name}\\",direction=\\"inbound\\"}'
            "[${linkerd_window}]))"
        ),
        "comparator": {
            "type": "float",
            "criteria": ">=",
            "value": "${degraded_success_rate_slo}",
        },
    },
}

# EOT promProbe: P99 latency recovers below absolute threshold.
_EOT_PROM_P99_RECOVERY: dict[str, Any] = {
    "name": "${target_name}-p99-recovery",
    "type": "promProbe",
    "mode": "EOT",
    "runProperties": {
        "probeTimeout": "5s",
        "interval": "${recovery_interval_s}s",
        "retry": "${recovery_retry}",
        "initialDelay": "${recovery_initial_delay_s}s",
        "stopOnFailure": False,
    },
    "promProbe/inputs": {
        "endpoint": "${prom_url}",
        "query": (
            'histogram_quantile(0.99, sum(rate('
            'response_latency_ms_bucket{deployment=\\"${target_name}\\",direction=\\"inbound\\"}'
            '[${linkerd_window}])) by (le))'
        ),
        "comparator": {
            "type": "float",
            "criteria": "<=",
            "value": "${p99_recovery_threshold_ms}",
        },
    },
}

# EOT promProbe: container restart count must not exceed max (memory-hog cascade detection).
_EOT_PROM_NO_RESTART_CASCADE: dict[str, Any] = {
    "name": "${target_name}-no-cascade-restart",
    "type": "promProbe",
    "mode": "EOT",
    "runProperties": {
        "probeTimeout": "5s",
        "interval": "${recovery_interval_s}s",
        "retry": "${recovery_retry}",
        "initialDelay": "${recovery_initial_delay_s}s",
        "stopOnFailure": False,
    },
    "promProbe/inputs": {
        "endpoint": "${prom_url}",
        "query": (
            "sum(increase(kube_pod_container_restarts_total{"
            'namespace=\\"${namespace}\\",pod=~\\"${target_name}-.*\\"}[3m]))'
        ),
        "comparator": {
            "type": "float",
            "criteria": "<=",
            "value": "${memory_restart_max}",
        },
    },
}

# OnChaos cmdProbe: confirms partition is active during network-loss (negative probe).
# Source-mode + hostNetwork:true keeps the probe pod resilient to the in-pod network drop.
_ONCHAOS_CMD_TARGET_UNREACHABLE: dict[str, Any] = {
    "name": "${target_name}-target-unreachable",
    "type": "cmdProbe",
    "mode": "OnChaos",
    "runProperties": {
        "probeTimeout": "4s",
        "interval": "10s",
        "retry": 1,
        "stopOnFailure": False,
    },
    "cmdProbe/inputs": {
        "command": (
            "curl -s -o /dev/null -w '%{http_code}' --max-time 3 "
            "http://${target_name}.${namespace}.svc.cluster.local:${target_port}"
            "${target_health_path} || echo 'TIMEOUT'"
        ),
        "comparator": {
            "type": "string",
            "criteria": "contains",
            "value": "TIMEOUT",
        },
        "source": {
            "image": "${cmd_probe_image}",
            "hostNetwork": True,
        },
    },
}

# OnChaos tcp connectivity probe: verify the target becomes unreachable.
_ONCHAOS_TCP_TARGET_UNREACHABLE: dict[str, Any] = {
    "name": "${target_name}-target-unreachable",
    "type": "cmdProbe",
    "mode": "OnChaos",
    "runProperties": {
        "probeTimeout": "4s",
        "interval": "10s",
        "retry": 1,
        "stopOnFailure": False,
    },
    "cmdProbe/inputs": {
        "command": _tcp_unreachable_command(),
        "comparator": {
            "type": "string",
            "criteria": "contains",
            "value": "TIMEOUT",
        },
        "source": {
            "image": "${tcp_probe_image}",
            "hostNetwork": True,
        },
    },
}

# EOT httpProbe: post-chaos service must reachable again (network-loss recovery).
_EOT_HTTP_RECOVERY: dict[str, Any] = {
    "name": "${target_name}-recovery-connectivity",
    "type": "httpProbe",
    "mode": "EOT",
    "runProperties": {
        "probeTimeout": "5s",
        "interval": "${recovery_interval_s}s",
        "retry": "${recovery_retry}",
        "initialDelay": "${recovery_initial_delay_s}s",
        "stopOnFailure": False,
    },
    "httpProbe/inputs": {
        "url": "http://${target_name}.${namespace}.svc.cluster.local:${target_port}${target_health_path}",
        "insecureSkipVerify": True,
        "method": {"get": {"criteria": "==", "responseCode": "200"}},
    },
}

# EOT tcp connectivity check: service must accept TCP connections again.
_EOT_TCP_RECOVERY: dict[str, Any] = {
    "name": "${target_name}-recovery-connectivity",
    "type": "cmdProbe",
    "mode": "EOT",
    "runProperties": {
        "probeTimeout": "5s",
        "interval": "${recovery_interval_s}s",
        "retry": "${recovery_retry}",
        "initialDelay": "${recovery_initial_delay_s}s",
        "stopOnFailure": False,
    },
    "cmdProbe/inputs": {
        "command": _tcp_connect_command(),
        "comparator": {
            "type": "string",
            "criteria": "contains",
            "value": "OK",
        },
        "source": {
            "image": "${tcp_probe_image}",
            "hostNetwork": True,
        },
    },
}


# ---------------------------------------------------------------------------
# Default probe-stack per fault type
# ---------------------------------------------------------------------------

DEFAULT_PROBE_TEMPLATES: dict[str, list[dict[str, Any]]] = {
    "pod-delete": [
        _SOT_HTTP_BASELINE,
        _CONTINUOUS_HTTP_LIVENESS,
        _EOT_K8S_DEPLOYMENT_PRESENT,
    ],
    "pod-cpu-hog": [
        _SOT_PROM_P99_BASELINE,
        _CONTINUOUS_PROM_SUCCESS_RATE,
        _EOT_PROM_P99_RECOVERY,
    ],
    "pod-memory-hog": [
        _SOT_HTTP_BASELINE,
        _CONTINUOUS_HTTP_LIVENESS,
        _EOT_PROM_NO_RESTART_CASCADE,
    ],
    "pod-network-latency": [
        _CONTINUOUS_HTTP_LIVENESS,
        _CONTINUOUS_PROM_DEGRADED_SUCCESS_RATE,
        _EOT_PROM_P99_RECOVERY,
    ],
    "pod-network-loss": [
        _SOT_HTTP_BASELINE,
        _ONCHAOS_CMD_TARGET_UNREACHABLE,
        _CONTINUOUS_PROM_DEGRADED_SUCCESS_RATE,
        _EOT_HTTP_RECOVERY,
    ],
}

DEFAULT_TCP_PROBE_TEMPLATES: dict[str, list[dict[str, Any]]] = {
    "pod-delete": [
        _SOT_TCP_BASELINE,
        _CONTINUOUS_TCP_LIVENESS,
        _EOT_K8S_DEPLOYMENT_PRESENT,
    ],
    "pod-cpu-hog": [
        _SOT_PROM_P99_BASELINE,
        _CONTINUOUS_PROM_SUCCESS_RATE,
        _EOT_PROM_P99_RECOVERY,
    ],
    "pod-memory-hog": [
        _SOT_TCP_BASELINE,
        _CONTINUOUS_TCP_LIVENESS,
        _EOT_PROM_NO_RESTART_CASCADE,
    ],
    "pod-network-latency": [
        _CONTINUOUS_TCP_LIVENESS,
        _CONTINUOUS_PROM_DEGRADED_SUCCESS_RATE,
        _EOT_PROM_P99_RECOVERY,
    ],
    "pod-network-loss": [
        _SOT_TCP_BASELINE,
        _ONCHAOS_TCP_TARGET_UNREACHABLE,
        _CONTINUOUS_PROM_DEGRADED_SUCCESS_RATE,
        _EOT_TCP_RECOVERY,
    ],
}


# ---------------------------------------------------------------------------
# Public builder
# ---------------------------------------------------------------------------


def _template_name_matches(
    template: dict[str, Any],
    disable_set: set[str],
    ctx: dict[str, Any],
) -> bool:
    """Resolve template name (which may contain ${var}) before checking disable list."""
    raw_name = template.get("name", "")
    rendered_name = Template(raw_name).safe_substitute(ctx)
    return raw_name in disable_set or rendered_name in disable_set


def build_probes(  # noqa: PLR0913
    *,
    experiment_type: str,
    namespace: str,
    target_label: str,
    target_port: int,
    service_protocol: str = "http",
    target_clusterip: str = "",
    prom_url: str | None,
    settings: Settings,
    deployment_thresholds: dict[str, Any] | None = None,
    experiment_configuration: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Resolve probe templates → concrete probe spec list ready for ChaosEngine.

    Returns empty list if experiment_type has no defaults and no override is supplied.
    PromProbes are skipped when prom_url is not available (cluster has no VictoriaMetrics).
    """
    overrides: ProbeOverrides = (experiment_configuration or {}).get("probes") or {}

    thresholds = resolve_thresholds(
        settings=settings,
        deployment_thresholds=deployment_thresholds,
        experiment_overrides=overrides.get("thresholds"),
    )

    ctx = {
        **thresholds,
        "namespace": namespace,
        "target_label": target_label,
        "target_name": _extract_target_name(target_label),
        "target_port": target_port,
        "target_clusterip": target_clusterip,
        "prom_url": prom_url or "",
        "tcp_probe_image": settings.PROBE_DEFAULT_TCP_CMD_PROBE_IMAGE,
    }

    if "override" in overrides and overrides["override"]:
        raw_templates: list[dict[str, Any]] = list(overrides["override"])
    else:
        registry = (
            DEFAULT_PROBE_TEMPLATES
            if service_protocol.lower().strip() == "http"
            else DEFAULT_TCP_PROBE_TEMPLATES
        )
        raw_templates = list(registry.get(experiment_type, []))
        disable = set(overrides.get("disable") or [])
        if disable:
            raw_templates = [
                t for t in raw_templates
                if not _template_name_matches(t, disable, ctx)
            ]
        if overrides.get("additional"):
            raw_templates.extend(overrides.get("additional") or [])

    rendered: list[dict[str, Any]] = []
    for tpl in raw_templates:
        spec = _render(copy.deepcopy(tpl), ctx)
        if isinstance(spec, dict):
            _coerce_probe_types(spec)
        if spec.get("type") == "promProbe" and not prom_url:
            logger.warning(
                "Skipping promProbe %s: cluster has no victoriametrics_url",
                spec.get("name"),
            )
            continue
        rendered.append(spec)

    return rendered
