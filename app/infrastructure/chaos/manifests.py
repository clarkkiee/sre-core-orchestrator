"""Kubernetes manifest builders for the Litmus Chaos set of experiments"""

from typing import Any

_LITMUSCHAOS_NS = "litmus"

_DEFAULT_LITMUS_IMAGE = "litmuschaos/go-runner:3.27.0"

# ---------------------------------------------------------------------------
# Experiment template registry
#
# Each entry defines the default tunables for one fault type.  Only include
# meaningful defaults here — empty-string optionals (TARGET_PODS, NODE_LABEL,
# etc.) are omitted to keep the templates readable.
#
# To add a new fault type:
#   1. Add an entry below following the same structure.
#   2. Add the matching ExperimentType enum value in app/models/chaos.py.
#
# Sources: https://hub.litmuschaos.io/api/chaos/master?file=faults/kubernetes/<name>/fault.yaml
# ---------------------------------------------------------------------------


EXPERIMENT_TEMPLATES: dict[str, dict[str, Any]] = {
    "pod-delete": {
        "args": "./experiments -name pod-delete",
        "env": {
            "TOTAL_CHAOS_DURATION": "120",
            "CHAOS_INTERVAL": "30",
            "FORCE": "true",
            "PODS_AFFECTED_PERC": "0",
            "SEQUENCE": "parallel",
            "RAMP_TIME": "0",
        },
    },
    "pod-cpu-hog": {
        "args": "./experiments -name pod-cpu-hog",
        "env": {
            "TOTAL_CHAOS_DURATION": "120",
            "CPU_CORES": "1",  # number of cores to stress
            "CPU_LOAD": "100",  # CPU load percentage per core
            "PODS_AFFECTED_PERC": "0",
            "SEQUENCE": "parallel",
            "CONTAINER_RUNTIME": "containerd",
            "SOCKET_PATH": "/run/k3s/containerd/containerd.sock",
            "RAMP_TIME":"0",
        },
    },
    "pod-memory-hog": {
        "args": "./experiments -name pod-memory-hog",
        "env": {
            "TOTAL_CHAOS_DURATION": "120",
            "MEMORY_CONSUMPTION": "500",  # MB to consume per worker
            "NUMBER_OF_WORKERS": "1",  # parallel stress workers
            "PODS_AFFECTED_PERC": "0",
            "SEQUENCE": "parallel",
            "CONTAINER_RUNTIME": "containerd",
            "SOCKET_PATH": "/run/k3s/containerd/containerd.sock",
            "RAMP_TIME":"0",
        },
    },
    "pod-network-latency": {
        "args": "./experiments -name pod-network-latency",
        "env": {
            "TOTAL_CHAOS_DURATION": "120",
            "NETWORK_LATENCY": "2000",
            "JITTER": "0",
            "NETWORK_INTERFACE": "eth0",
            "PODS_AFFECTED_PERC": "0",
            "SEQUENCE": "parallel",
            "CONTAINER_RUNTIME": "containerd",
            "SOCKET_PATH": "/run/k3s/containerd/containerd.sock",
            "RAMP_TIME":"0",
        },
    },
    "pod-network-loss": {
        "args": "./experiments -name pod-network-loss",
        "env": {
            "TOTAL_CHAOS_DURATION": "120",
            "NETWORK_PACKET_LOSS_PERCENTAGE": "100",  # 0-100%
            "NETWORK_INTERFACE": "eth0",
            "PODS_AFFECTED_PERC": "0",
            "SEQUENCE": "parallel",
            "CONTAINER_RUNTIME": "containerd",
            "SOCKET_PATH": "/run/k3s/containerd/containerd.sock",
          
            "RAMP_TIME":"0",
        },
    },
}


def build_litmuschaos_namespace() -> dict[str, Any]:
    """Return a Namespace manifest for the litmus operator."""
    return {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": _LITMUSCHAOS_NS},
    }


def build_namespaced_litmuschaos_rbac(namespace: str) -> list[dict[str, Any]]:
    """Return [ServiceAccount, Role, RoleBinding] scoped to the target namespace.

    Must be applied once per target namespace before running any experiment.
    """
    sa_name = "litmus-runner"

    sa: dict[str, Any] = {
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {
            "name": sa_name,
            "namespace": namespace,
            "labels": {
                "name": sa_name,
                "app.kubernetes.io/part-of": "litmus",
            },
        },
    }

    role: dict[str, Any] = {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "Role",
        "metadata": {
            "name": sa_name,
            "namespace": namespace,
            "labels": {
                "name": sa_name,
                "app.kubernetes.io/part-of": "litmus",
            },
        },
        "rules": [
            {
                "apiGroups": [""],
                "resources": ["pods"],
                "verbs": [
                    "create",
                    "delete",
                    "get",
                    "list",
                    "patch",
                    "update",
                    "deletecollection",
                ],
            },
            {
                "apiGroups": [""],
                "resources": ["events"],
                "verbs": ["create", "get", "list", "patch", "delete"],
            },
            {
                "apiGroups": [""],
                "resources": ["configmaps"],
                "verbs": ["get", "list"],
            },
            {
                "apiGroups": [""],
                "resources": ["pods/log"],
                "verbs": ["get", "list", "watch"],
            },
            {
                "apiGroups": [""],
                "resources": ["pods/exec"],
                "verbs": ["create", "get", "list"],
            },
            {
                "apiGroups": ["apps"],
                "resources": [
                    "deployments",
                    "statefulsets",
                    "daemonsets",
                    "replicasets",
                ],
                "verbs": ["list", "get"],
            },
            {
                "apiGroups": [""],
                "resources": ["replicationcontrollers"],
                "verbs": ["list", "get"],
            },
            {
                "apiGroups": ["batch"],
                "resources": ["jobs"],
                "verbs": ["create", "list", "get", "delete", "deletecollection"],
            },
            {
                "apiGroups": ["litmuschaos.io"],
                "resources": ["chaosengines", "chaosexperiments", "chaosresults"],
                "verbs": ["get", "list", "create", "patch", "delete", "update"],
            },
        ],
    }

    role_binding: dict[str, Any] = {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": {
            "name": sa_name,
            "namespace": namespace,
            "labels": {
                "name": sa_name,
                "app.kubernetes.io/part-of": "litmus",
            },
        },
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "Role",
            "name": sa_name,
        },
        "subjects": [
            {
                "kind": "ServiceAccount",
                "name": sa_name,
                "namespace": namespace,
            }
        ],
    }

    return [sa, role, role_binding]


_NEEDS_RUNTIME_SOCKET = {
    "pod-cpu-hog",
    "pod-memory-hog",
    "pod-network-latency",
    "pod-network-loss",
}


def build_chaos_experiment(
    experiment_type: str,
    namespace: str,
    litmus_image: str = _DEFAULT_LITMUS_IMAGE,
) -> dict[str, Any]:
    """Return a ChaosExperiment CR for the given fault type in the target namespace.

    Raises KeyError if experiment_type is not in EXPERIMENT_TEMPLATES.
    """
    template = EXPERIMENT_TEMPLATES[experiment_type]
    env_list = [{"name": k, "value": v} for k, v in template["env"].items()]

    definition: dict[str, Any] = {
        "scope": "Namespaced",
        "image": litmus_image,
        "command": ["/bin/bash"],
        "args": ["-c", template["args"]],
        "env": env_list,
        "labels": {
            "name": experiment_type,
            "app.kubernetes.io/part-of": "litmus",
            "app.kubernetes.io/component": "experiment-job",
        },
    }

    if experiment_type in _NEEDS_RUNTIME_SOCKET:
        definition["hostPID"] = True
        definition["permissions"] = [
            {
                "apiGroups": [""],
                "resources": ["pods"],
                "verbs": ["get", "list", "delete", "deletecollection"],
            },
        ]
        definition["securityContext"] = {
            "podSecurityContext": {
                "runAsUser": 0,
                "runAsGroup": 0,
            },
            "containerSecurityContext": {
                "privileged": True,
                "allowPrivilegeEscalation": True,
            },
        }

    return {
        "apiVersion": "litmuschaos.io/v1alpha1",
        "kind": "ChaosExperiment",
        "metadata": {
            "name": experiment_type,
            "namespace": namespace,
            "labels": {
                "name": experiment_type,
                "app.kubernetes.io/part-of": "litmus",
            },
        },
        "spec": {
            "definition": definition,
        },
    }


# Configuration keys consumed by other subsystems (e.g. probe builder) — not env vars.
_RESERVED_CONFIG_KEYS = {"probes"}


def _build_engine_env(
    experiment_type: str,
    duration: int,
    configuration: dict[str, Any] | None = None,
) -> list[dict[str, str]]:
    """Build the full env list for a ChaosEngine experiment.

    Merge order (highest priority last):
      1. Template defaults from EXPERIMENT_TEMPLATES
      2. Duration override
      3. User-provided configuration overrides (excluding _RESERVED_CONFIG_KEYS)
    """
    template = EXPERIMENT_TEMPLATES[experiment_type]
    env = dict(template["env"])
    env["TOTAL_CHAOS_DURATION"] = str(duration)
    if configuration:
        for key, value in configuration.items():
            if key in _RESERVED_CONFIG_KEYS:
                continue
            env[key] = str(value).lower() if isinstance(value, bool) else str(value)
    return [{"name": k, "value": v} for k, v in env.items()]


def build_chaos_engine(  # noqa: PLR0913
    engine_name: str,
    namespace: str,
    app_label: str,
    experiment_type: str,
    duration: int,
    configuration: dict[str, Any] | None = None,
    probes: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Return a ChaosEngine CR that triggers the experiment against the target app.

    Args:
        engine_name:     Unique name for this engine run (e.g. "pod-delete-abc123").
        namespace:       Target namespace where the app and ChaosExperiment CR live.
        app_label:       Label selector for the target pods (e.g. "app=frontend").
        experiment_type: Must match a ChaosExperiment CR name in the same namespace.
        duration:        Overrides TOTAL_CHAOS_DURATION (seconds).
        configuration:   Optional env-var overrides (e.g. TARGET_CONTAINER).
        probes:          Optional list of Litmus probe specs. When provided, injected
                         into experiments[0].spec.components.probe — used as a binary
                         gate alongside built-in checks for ChaosResult verdict.
    """
    components: dict[str, Any] = {
        "env": _build_engine_env(experiment_type, duration, configuration),
        "experimentAnnotations": {
            "linkerd.io/inject": "disabled",
        },
    }

    experiment_spec: dict[str, Any] = {"components": components}
    if probes:
        experiment_spec["probe"] = probes

    return {
        "apiVersion": "litmuschaos.io/v1alpha1",
        "kind": "ChaosEngine",
        "metadata": {
            "name": engine_name,
            "namespace": namespace,
        },
        "spec": {
            "appinfo": {
                "appns": namespace,
                "applabel": app_label,
                "appkind": "deployment",
            },
            "engineState": "active",
            "chaosServiceAccount": "litmus-runner",
            "components": {
                "runner": {
                    "runnerAnnotation": {
                        "linkerd.io/inject": "disabled",
                    },
                },
            },
            "experiments": [
                {
                    "name": experiment_type,
                    "spec": experiment_spec,
                }
            ],
        },
    }

def build_chaos_exporter(
    namespace: str
) -> list[dict[str, Any]]:

    labels = {"app": "chaos-monitor"}

    sa = {
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {
            "name": "chaos-exporter",
            "namespace": namespace,
            "labels": labels
        }
    }

    cluster_role = {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRole",
        "metadata": {
            "name": "chaos-exporter",
            "labels": labels
        },
        "rules": [
            {
                "apiGroups": ["litmuschaos.io"],
                "resources": ["chaosengines", "chaosresults", "chaosexperiments"],
                "verbs": ["get", "list", "watch"]
            },
            {
                "apiGroups": [""],
                "resources": ["pods", "events"],
                "verbs": ["get", "list", "watch"]
            },
        ],
    }

    cluster_role_binding = {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRoleBinding",
        "metadata": {
            "name": "chaos-exporter",
            "labels": labels,
        },
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": "chaos-exporter",
        },
        "subjects": [
            {
                "kind": "ServiceAccount",
                "name": "chaos-exporter",
                "namespace": namespace,
            }
        ]
    }

    deployment = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "labels": labels,
            "name": "chaos-monitor",
            "namespace": namespace,
        },
        "spec": {
            "replicas": 1,
            "selector": {
                "matchLabels": labels
            },
            "template": {
                "metadata": {
                    "labels": labels
                },
                "spec": {
                    "containers": [
                        {
                            "image": "litmuschaos/chaos-exporter:3.28.0",
                            "imagePullPolicy": "IfNotPresent",
                            "ports": [{"containerPort": 8080, "name": "http-metrics"}],
                            "name": "chaos-exporter",
                            "env": [
                                {
                                    "name": "WATCH_NAMESPACE",
                                    "value": ""
                                },
                                {
                                    "name": "TSDB_SCRAPE_INTERVAL",
                                    "value": "10"
                                }
                            ]
                        }
                    ],
                    "serviceAccountName": "chaos-exporter"
                }
            }
        }
    }

    service = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "labels": labels,
            "namespace": namespace,
            "name": "chaos-monitor"
        },
        "spec": {
            "ports": [
                {
                    "port": 8080,
                    "protocol": "TCP",
                    "targetPort": 8080,
                    "name": "http-metrics",
                }
            ],
            "selector": labels
        }
    }

    return [sa, cluster_role, cluster_role_binding, deployment, service]
