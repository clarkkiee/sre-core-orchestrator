from __future__ import annotations

import copy
import logging
from string import Template
from typing import Any, TypedDict

from app.utils.config import Settings

logger = logging.getLogger(__name__)

class ProbeOverrides(TypedDict, total=False):
    override: list[dict[str, Any]]
    additional: list[dict[str, Any]]
    disable: list[str]
    thresholds: dict[str, Any]
    
def _global_thresholds(settings: Settings) -> dict[str, Any]:
    return {
        "target_port": settings.PROBE_DEFAULT_TARGET_PORT,
        "target_health_path": settings.PROBE_DEFAULT_TARGET_HEALTH_PATH,
        "p99_baseline_threshold_ms": settings.PROBE_DEFAULT_P99_BASELINE_THRESHOLD_MS,
        "p99_recovery_threshold_ms": settings.PROBE_DEFAULT_P99_RECOVERY_THRESHOLD_MS,
        "linkerd_window": settings.PROBE_DEFAULT_LINKERD_WINDOW,
        "liveness_timeout_s": settings.PROBE_DEFAULT_LIVENESS_TIMEOUT_S,
        "liveness_poll_s": settings.PROBE_DEFAULT_LIVENESS_POLL_S,
        "recovery_initial_delay_s": settings.PROBE_DEFAULT_RECOVERY_INITIAL_DELAY_S,
        "recovery_retry": settings.PROBE_DEFAULT_RECOVERY_RETRY,
        "recovery_interval_s": settings.PROBE_DEFAULT_RECOVERY_INTERVAL_S,
        "memory_restart_max": settings.PROBE_DEFAULT_MEMORY_RESTART_MAX,
        "cmd_probe_image": settings.PROBE_DEFAULT_CMD_PROBE_IMAGE,
        "tcp_probe_image": settings.PROBE_DEFAULT_TCP_CMD_PROBE_IMAGE,
        "kubectl_probe_image": settings.PROBE_DEFAULT_KUBECTL_PROBE_IMAGE
    }
    
# HELPERS

def _tcp_connect_command() -> str:
    return "nc -z -w 3 ${target_clusterip} ${target_port} && echo 'OK' || echo 'FAIL'"
    
def _tcp_unreachable_command() -> str:
    return "nc -z -w 3 ${target_clusterip} ${target_port} && echo 'REACHABLE' || echo 'TIMEOUT'"

# SOT PROBES UNTUK BASELINE 
_SOT_HTTP_BASELINE: dict[str, Any] = {
    "name": "${target_name}-baseline-guard",
    "type": "httpProbe",
    "mode": "SOT",
    "runProperties": {
        "probeTimeout": "5s",
        "interval": "2s",
        "retry": 1,
        "stopOnFailure": True
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
    }
}

_SOT_TCP_BASELINE: dict[str, Any] = {
    "name": "${target_name}-baseline-guard",
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
            "value": "OK"
        },
        "source": {
            "image": "${tcp_probe_image}",
            "hostNetwork": True
        }
    }
}


_SOT_PROM_P99_BASELINE: dict[str, Any] = {
    "name": "${target_name}-baseline-latency-p99",
    "type": "promProbe",
    "mode": "SOT",
    "runProperties": {
        "probeTimeout": "5s",
        "interval": "5s",
        "retry": 1,
        "stopOnFailure": False,
    },
    "promProbe/inputs": {
        "endpoint": "${prom_url}",
        "query": """
            histogram_quantile(0.99, 
                sum(
                    rate(response_latency_ms_bucket{pod=~'${target_name}-[a-z0-9]+-[a-z0-9]+',direction='inbound'}[${linkerd_window}])
                ) by (le)
            ) or vector(0)
        """.strip(),
        "comparator": {
            "type": "float",
            "criteria": "<=",
            "value": "${p99_baseline_threshold_ms}",
        }
    }
}

_SOT_PROM_ERROR_RATE_BASELINE: dict[str, Any] = {
    "name": "${target_name}-baseline-error-rate",
    "type": "promProbe",
    "mode": "SOT",
    "runProperties": {
        "probeTimeout": "5s",
        "interval": "5s",
        "retry": 1,
        "stopOnFailure": False,
    },
    "promProbe/inputs": {
        "endpoint": "${prom_url}",
        "query": """
            (sum(rate(response_total{pod=~'${target_name}-[a-z0-9]+-[a-z0-9]+', direction='inbound', classification!='success'}[120s]))
            /
            sum(rate(response_total{pod=~'${target_name}-[a-z0-9]+-[a-z0-9]+', direction='inbound'}[120s]))) or vector(0)
        """,
        "comparator": {
            "type": "float",
            "criteria": "<=",
            "value": "1.0"
        }
    }
}

_CONTINUOUS_HTTP_LIVENESS: dict[str, Any] = {
    "name": "${target_name}-fault-availability",
    "type": "httpProbe",
    "mode": "Continuous",   
    "runProperties": {
        "probeTimeout": "${liveness_timeout_s}s",
        "interval": "${liveness_poll_s}s",
        "retry": 0,
        "stopOnFailure": False,
        "probePollingInterval": "${liveness_poll_s}s",
    },
    "httpProbe/inputs": {
        "url": "http://${target_name}.${namespace}.svc.cluster.local:${target_port}${target_health_path}",
        "insecureSkipVerify": True,
        "method": {
            "get": {
                "criteria": "==",
                "responseCode": "200"
            }
        }
    }
}

_CONTINUOUS_TCP_LIVENESS: dict[str, Any] = {
    "name": "${target_name}-fault-availability",
    "type": "cmdProbe",
    "mode": "Continuous",
    "runProperties": {
        "probeTimeout": "${liveness_timeout_s}s",
        "interval": "${liveness_poll_s}s",
        "retry": 0,
        "stopOnFailure": False,
        "probePollingInterval": "${liveness_poll_s}s",
    },
    "cmdProbe/inputs": {
        "command": _tcp_connect_command(),
        "comparator": {
            "type": "string",
            "criteria": "contains",
            "value": "OK"
        },
        "source": {
            "image": "${tcp_probe_image}",
            "hostNetwork": True,
        }
    }
}

_EOT_FAULT_LATENCY_P99: dict[str, Any] = {
    "name": "${target_name}-fault-latency-p99",
    "type": "promProbe",
    "mode": "EOT",
    "runProperties": {
        "probeTimeout": "5s",
        "interval": "5s",
        "retry": 1,
        "stopOnFailure": False,
        "initialDelay": "0s",
    },
    "promProbe/inputs": {
        "endpoint": "${prom_url}",
        "query": """
            histogram_quantile(
                0.99,
                sum(rate(response_latency_ms_bucket{pod=~'${target_name}-[a-z0-9]+-[a-z0-9]+', direction='inbound'}[120s])) by (le)
            ) or vector(0)
        """,
        "comparator": {
            "type": "float",
            "criteria": "<=",
            "value": "99999",
        }
    }
}

_EOT_FAULT_ERROR_RATE: dict[str, Any] = {
    "name": "${target_name}-fault-error-rate",
    "type": "promProbe",
    "mode": "EOT",
    "runProperties": {
        "probeTimeout": "5s",
        "interval": "5s",
        "retry": 1,
        "initialDelay": "0s",
        "stopOnFailure": False,
    },
    "promProbe/inputs": {
        "endpoint": "${prom_url}",
        "query": """
            (sum(rate(response_total{pod=~'${target_name}-[a-z0-9]+-[a-z0-9]+', direction='inbound', classification!='success'}[120s]))
            /
            sum(rate(response_total{pod=~'${target_name}-[a-z0-9]+-[a-z0-9]+', direction='inbound'}[120s]))) or vector(0)
        """,
        "comparator": {
            "type": "float",
            "criteria": "<=",
            "value": "99999",
        }
    }
}

_EOT_PROM_LATENCY_P99_RECOVERY: dict[str, Any] = {
    "name": "${target_name}-recovery-latency-p99",
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
        "query": """
            histogram_quantile(
                0.99,
                sum(rate(response_latency_ms_bucket{pod=~'${target_name}-[a-z0-9]+-[a-z0-9]+', direction='inbound'}[120s])) by (le)
            ) or vector(0)
        """,
        "comparator": {
            "type": "float",
            "criteria": "<=",
            "value": "${p99_recovery_threshold_ms}",
        }
    }
}

_EOT_HTTP_RECOVERY: dict[str, Any] = {
    "name": "${target_name}-recovery-availability",
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
        "method": {
            "get": {
                "criteria": "==",
                "responseCode": "200"
            }
        }
    }
}

_EOT_TCP_RECOVERY: dict[str, Any] = {
    "name": "${target_name}-recovery-availability",
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
            "value": "OK"
        },
        "source": {
            "image": "${tcp_probe_image}",
            "hostNetwork": True
        }
    }
}

_EOT_K8S_DEPLOYMENT_PRESENT: dict[str, Any] = {
    "name": "${target_name}-replica-restored",
    "type": "k8sProbe",
    "mode": "EOT",
    "runProperties": {
        "probeTimeout": "5s",
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
    }
}

_EOT_PROM_NO_RESTART_CASCADE: dict[str, Any] = {
    "name": "${target_name}-restart-cascade",
    "type": "promProbe",
    "mode": "EOT",
    "runProperties": {
        "probeTimeout": "5s",
        "interval": "${recovery_interval_s}s",
        "retry": "${recovery_retry}",
        "initialDelay": "0s",
        "stopOnFailure": False,
    },
    "promProbe/inputs": {
        "endpoint": "${prom_url}",
        "query": """
            sum(increase(kube_pod_container_restarts_total{namespace="${namespace}", pod=~"${target_name}-.*"}[120s]))
        """,
        "comparator": {
            "type": "float",
            "criteria": "<=",
            "value": "${memory_restart_max}"
        }
    }
}

_ONCHAOS_CMD_TARGET_UNREACHABLE: dict[str, Any] = {
    "name": "${target_name}-partition-active",
    "type": "cmdProbe",
    "mode": "OnChaos",
    "runProperties": {
        "probeTimeout": "4s",
        "interval": "10s",
        "retry": 1,
        "stopOnFailure": False,
    },
    "cmdProbe/inputs": {
        "command": """
            curl -s -o /dev/null -w '%{http_code}' --max-time 3 http://${target_name}.${namespace}.svc.cluster.local:${target_port}${target_health_path} || echo 'TIMEOUT'
        """,
        "comparator": {
            "type": "string",
            "criteria": "contains",
            "value": "TIMEOUT"
        },
        "source": {
            "image": "${cmd_probe_image}",
            "hostNetwork": True
        }
    }
}

_ONCHAOS_TCP_TARGET_UNREACHABLE: dict[str, Any] = {
    "name": "${target_name}-partition-active",
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
            "value": "TIMEOUT"
        },
        "source": {
            "image": "${tcp_probe_image}",
            "hostNetwork": True
        }
    }
}


_BASE_HTTP = [
    _SOT_HTTP_BASELINE,
    _SOT_PROM_P99_BASELINE,
    _SOT_PROM_ERROR_RATE_BASELINE,
    _CONTINUOUS_HTTP_LIVENESS,
    _EOT_FAULT_LATENCY_P99,
    _EOT_FAULT_ERROR_RATE,
    _EOT_PROM_LATENCY_P99_RECOVERY,
    _EOT_HTTP_RECOVERY    
]

_BASE_TCP = [
    _SOT_TCP_BASELINE,
    _SOT_PROM_P99_BASELINE,
    _SOT_PROM_ERROR_RATE_BASELINE,
    _CONTINUOUS_TCP_LIVENESS,
    _EOT_FAULT_LATENCY_P99,
    _EOT_FAULT_ERROR_RATE,
    _EOT_PROM_LATENCY_P99_RECOVERY,
    _EOT_TCP_RECOVERY
]

DEFAULT_PROBE_TEMPLATES: dict[str, list[dict[str, Any]]] = {
    "pod-delete": [*_BASE_HTTP, _EOT_K8S_DEPLOYMENT_PRESENT],
    "pod-cpu-hog": [*_BASE_HTTP],
    "pod-memory-hog": [*_BASE_HTTP, _EOT_PROM_NO_RESTART_CASCADE],
    "pod-network-latency": [*_BASE_HTTP],
    "pod-network-loss": [
        _SOT_HTTP_BASELINE,
        _SOT_PROM_P99_BASELINE,
        _SOT_PROM_ERROR_RATE_BASELINE,
        _ONCHAOS_CMD_TARGET_UNREACHABLE,
        _CONTINUOUS_HTTP_LIVENESS,
        _EOT_FAULT_LATENCY_P99,
        _EOT_FAULT_ERROR_RATE,
        _EOT_PROM_LATENCY_P99_RECOVERY,
        _EOT_HTTP_RECOVERY,
    ]
}

DEFAULT_TCP_PROBE_TEMPLATES: dict[str, list[dict[str, Any]]] = {
    "pod-delete": [*_BASE_TCP, _EOT_K8S_DEPLOYMENT_PRESENT],
    "pod-cpu-hog": [*_BASE_TCP],
    "pod-memory-hog": [*_BASE_TCP, _EOT_PROM_NO_RESTART_CASCADE],
    "pod-network-latency": [*_BASE_TCP],
    "pod-network-loss": [
        _SOT_TCP_BASELINE,
        _SOT_PROM_P99_BASELINE,
        _SOT_PROM_ERROR_RATE_BASELINE,
        _ONCHAOS_TCP_TARGET_UNREACHABLE,
        _CONTINUOUS_TCP_LIVENESS,
        _EOT_FAULT_LATENCY_P99,
        _EOT_FAULT_ERROR_RATE,
        _EOT_PROM_LATENCY_P99_RECOVERY,
        _EOT_TCP_RECOVERY,
    ]
}

def resolve_thresholds(
    *,
    settings: Settings,
    deployment_thresholds: dict[str, Any] | None = None,
    experiment_overrides: dict[str, Any] | None = None
) -> dict[str, Any]:
    merged = _global_thresholds(settings)
    if deployment_thresholds:
        merged.update(deployment_thresholds)
    if experiment_overrides:
        merged.update(experiment_overrides)
    return merged

def _extract_target_name(app_label: str) -> str:
    if "=" not in app_label:
        return app_label
    return app_label.split("=",1)[1].strip()

def _render(value: Any, ctx: dict[str, Any]) -> Any: # noqa: ANN401
    if isinstance(value, str):
        try:
            return Template(value).safe_substitute(ctx)
        except (KeyError, ValueError) as exc:
            logger.warning("Probe template substitution failed for %r: %s", value, exc)
            return value
    if isinstance(value, dict):
        return {k: _render(v, ctx) for k, v in value.items()}
    if isinstance(value, list):
        return [_render(item, ctx) for item in value]
    return value

def _template_name_matches(
    template: dict[str, Any],
    disable_set: set[str],
    ctx: dict[str, Any]
) -> bool:
    raw_name = template.get("name", "")
    rendered_name = Template(raw_name).safe_substitute(ctx)
    return raw_name in disable_set or rendered_name in disable_set

def _coerce_probe_types(spec: dict[str, Any]) -> None:
    run_properties = spec.get("runProperties")
    if not isinstance(run_properties, dict):
        return
    retry = run_properties.get("retry")
    if isinstance(retry, str) and retry.isdigit():
        run_properties["retry"] = int(retry)


def _has_unresolved_template(value: Any) -> bool:  # noqa: ANN401
    if isinstance(value, str):
        return "${" in value
    if isinstance(value, dict):
        return any(_has_unresolved_template(v) for v in value.values())
    if isinstance(value, list):
        return any(_has_unresolved_template(v) for v in value)
    return False


def _seconds(value: Any, default: int) -> int:
    if isinstance(value, (int, float)):
        return max(0, int(value))
    if isinstance(value, str):
        raw = value.strip()
        if raw.endswith("s"):
            raw = raw[:-1].strip()
        if raw.isdigit():
            return int(raw)
    return default


def estimate_eot_probe_overhead_seconds(probes: list[dict[str, Any]]) -> int:
    """Estimate worst-case additional runtime from all rendered EOT probes.

    Litmus EOT probes may run after the chaos duration finishes. When many EOT probes
    are configured, a static timeout can under-estimate total runtime and trigger
    false experiment timeouts.
    """
    total = 0
    eot_breakdown = []
    
    for probe in probes:
        if str(probe.get("mode", "")).upper() != "EOT":
            continue

        rp = probe.get("runProperties")
        if not isinstance(rp, dict):
            continue

        initial_delay_s = _seconds(rp.get("initialDelay"), 0)
        interval_s = _seconds(rp.get("interval"), 10)
        probe_timeout_s = _seconds(rp.get("probeTimeout"), 5)
        retry = rp.get("retry", 0)
        retry_count = int(retry) if isinstance(retry, int) else _seconds(retry, 0)

        # Conservative upper bound that matches the existing timeout formula semantics.
        per_probe = initial_delay_s + probe_timeout_s + (
            retry_count * (interval_s + probe_timeout_s)
        )
        total += per_probe
        eot_breakdown.append({
            "name": probe.get("name", "unknown"),
            "initialDelay": initial_delay_s,
            "probeTimeout": probe_timeout_s,
            "interval": interval_s,
            "retry": retry_count,
            "calculated": per_probe,
        })

    if eot_breakdown:
        logger.info(
            "EOT probe overhead breakdown (total=%ss): %s",
            total,
            eot_breakdown,
        )
    
    return total

def build_probes(
    *,
    experiment_type: str,
    namespace: str,
    target_label: str,
    target_port: str,
    service_protocol: str = "http",
    target_clusterip: str = "",
    prom_url: str | None,
    settings: Settings,
    deployment_thresholds: dict[str, Any] | None = None,
    experiment_configuration: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    
    overrides: ProbeOverrides = (experiment_configuration or {}).get("probes") or {}
    
    thresholds = resolve_thresholds(
        experiment_overrides=overrides.get("thresholds"),
        deployment_thresholds=deployment_thresholds,
        settings=settings,
    )
    
    ctx = {
        **thresholds,
        "namespace": namespace,
        "target_label": target_label,
        "target_name": _extract_target_name(target_label),
        "target_port": target_port,
        "target_clusterip": target_clusterip,
        "prom_url": prom_url or "",
        "tcp_probe_image": settings.PROBE_DEFAULT_TCP_CMD_PROBE_IMAGE
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
        if _has_unresolved_template(spec):
            logger.warning(
                "Skipping probe with unresolved template values: %s",
                spec.get("name", "unknown") if isinstance(spec, dict) else "unknown",
            )
            continue
        if spec.get("type") == "promProbe" and not prom_url:
            logger.warning(
                "Skipping promProbe %s: cluster has no victoriametrics_url",
                spec.get("name"),
            )
            continue
        
        # Log rendered probe details for diagnostics
        if spec.get("mode", "").upper() == "EOT":
            rp = spec.get("runProperties", {})
            logger.debug(
                "Rendered EOT probe %s: initialDelay=%s interval=%s probeTimeout=%s retry=%s",
                spec.get("name"),
                rp.get("initialDelay"),
                rp.get("interval"),
                rp.get("probeTimeout"),
                rp.get("retry"),
            )
        
        rendered.append(spec)
        
    return rendered