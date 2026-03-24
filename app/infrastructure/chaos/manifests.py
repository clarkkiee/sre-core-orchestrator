"""Kubernetes manifest builders for the Litmus Chaos set of experiments"""

from typing import Any

_LITMUSCHAOS_NS = "litmus-chaos"

_LITMUS_IMAGE = "litmuschaos/go-runner:3.9.0"

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
            "TOTAL_CHAOS_DURATION": "15",  # seconds — how long to keep deleting
            "CHAOS_INTERVAL": "5",  # seconds between each deletion
            "FORCE": "true",  # forceful (true) vs graceful (false)
            "PODS_AFFECTED_PERC": "0",  # 0 = one pod; >0 = percentage of matching pods
            "SEQUENCE": "parallel",  # parallel | serial
        },
    },
    "pod-cpu-hog": {
        "args": "./experiments -name pod-cpu-hog",
        "env": {
            "TOTAL_CHAOS_DURATION": "60",
            "CPU_CORES": "1",  # number of cores to stress
            "CPU_LOAD": "100",  # CPU load percentage per core
            "PODS_AFFECTED_PERC": "0",
            "SEQUENCE": "parallel",
            "CONTAINER_RUNTIME": "containerd",
            "SOCKET_PATH": "/run/containerd/containerd.sock",
        },
    },
    "pod-memory-hog": {
        "args": "./experiments -name pod-memory-hog",
        "env": {
            "TOTAL_CHAOS_DURATION": "60",
            "MEMORY_CONSUMPTION": "500",  # MB to consume per worker
            "NUMBER_OF_WORKERS": "1",  # parallel stress workers
            "PODS_AFFECTED_PERC": "0",
            "SEQUENCE": "parallel",
            "CONTAINER_RUNTIME": "containerd",
            "SOCKET_PATH": "/run/containerd/containerd.sock",
        },
    },
    "pod-network-latency": {
        "args": "./experiments -name pod-network-latency",
        "env": {
            "TOTAL_CHAOS_DURATION": "60",
            "NETWORK_LATENCY": "2000",  # ms of added latency
            "JITTER": "0",  # ms of random jitter on top of latency
            "NETWORK_INTERFACE": "eth0",
            "PODS_AFFECTED_PERC": "0",
            "SEQUENCE": "parallel",
            "CONTAINER_RUNTIME": "containerd",
            "SOCKET_PATH": "/run/containerd/containerd.sock",
        },
    },
    "pod-network-loss": {
        "args": "./experiments -name pod-network-loss",
        "env": {
            "TOTAL_CHAOS_DURATION": "60",
            "NETWORK_PACKET_LOSS_PERCENTAGE": "100",  # 0-100%
            "NETWORK_INTERFACE": "eth0",
            "PODS_AFFECTED_PERC": "0",
            "SEQUENCE": "parallel",
            "CONTAINER_RUNTIME": "containerd",
            "SOCKET_PATH": "/run/containerd/containerd.sock",
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


def build_chaos_experiment(experiment_type: str, namespace: str) -> dict[str, Any]:
    """Return a ChaosExperiment CR for the given fault type in the target namespace.

    Raises KeyError if experiment_type is not in EXPERIMENT_TEMPLATES.
    """
    template = EXPERIMENT_TEMPLATES[experiment_type]
    env_list = [{"name": k, "value": v} for k, v in template["env"].items()]
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
            "definition": {
                "scope": "Namespaced",
                "image": _LITMUS_IMAGE,
                "command": ["/bin/bash"],
                "args": ["-c", template["args"]],
                "env": env_list,
            }
        },
    }


def build_chaos_engine(
    engine_name: str,
    namespace: str,
    app_label: str,
    experiment_type: str,
    duration: int,
) -> dict[str, Any]:
    """Return a ChaosEngine CR that triggers the experiment against the target app.

    Args:
        engine_name:     Unique name for this engine run (e.g. "pod-delete-abc123").
        namespace:       Target namespace where the app and ChaosExperiment CR live.
        app_label:       Label selector for the target pods (e.g. "app=frontend").
        experiment_type: Must match a ChaosExperiment CR name in the same namespace.
        duration:        Overrides TOTAL_CHAOS_DURATION (seconds).
    """
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
            "experiments": [
                {
                    "name": experiment_type,
                    "spec": {
                        "components": {
                            "env": [
                                {
                                    "name": "TOTAL_CHAOS_DURATION",
                                    "value": str(duration),
                                },
                            ]
                        }
                    },
                }
            ],
        },
    }
