"""Kubernetes manifest builders for the in-cluster monitoring stack."""

from typing import Any

_MONITORING_NS = "monitoring"
_KSM_LABEL = "kube-state-metrics"
_VM_LABEL = "victoria-metrics"
_KSM_PORT = 8080
_VM_PORT = 8428


def build_monitoring_namespace() -> dict[str, Any]:
    """Return a Namespace manifest for the monitoring stack."""
    return {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": _MONITORING_NS},
    }


# -- kube-state-metrics -------------------------------------------------------


def build_kube_state_metrics(image: str) -> list[dict[str, Any]]:
    """Return all manifests needed for kube-state-metrics."""
    sa: dict[str, Any] = {
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {
            "name": _KSM_LABEL,
            "namespace": _MONITORING_NS,
        },
    }

    cluster_role: dict[str, Any] = {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRole",
        "metadata": {"name": _KSM_LABEL},
        "rules": [
            {
                "apiGroups": [""],
                "resources": [
                    "configmaps",
                    "secrets",
                    "nodes",
                    "pods",
                    "services",
                    "serviceaccounts",
                    "resourcequotas",
                    "replicationcontrollers",
                    "limitranges",
                    "persistentvolumeclaims",
                    "persistentvolumes",
                    "namespaces",
                    "endpoints",
                ],
                "verbs": ["list", "watch"],
            },
            {
                "apiGroups": ["apps"],
                "resources": [
                    "statefulsets",
                    "daemonsets",
                    "deployments",
                    "replicasets",
                ],
                "verbs": ["list", "watch"],
            },
            {
                "apiGroups": ["batch"],
                "resources": ["cronjobs", "jobs"],
                "verbs": ["list", "watch"],
            },
            {
                "apiGroups": ["autoscaling"],
                "resources": ["horizontalpodautoscalers"],
                "verbs": ["list", "watch"],
            },
            {
                "apiGroups": ["networking.k8s.io"],
                "resources": [
                    "networkpolicies",
                    "ingresses",
                ],
                "verbs": ["list", "watch"],
            },
            {
                "apiGroups": ["coordination.k8s.io"],
                "resources": ["leases"],
                "verbs": ["list", "watch"],
            },
        ],
    }

    crb: dict[str, Any] = {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRoleBinding",
        "metadata": {"name": _KSM_LABEL},
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": _KSM_LABEL,
        },
        "subjects": [
            {
                "kind": "ServiceAccount",
                "name": _KSM_LABEL,
                "namespace": _MONITORING_NS,
            },
        ],
    }

    deployment: dict[str, Any] = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": _KSM_LABEL,
            "namespace": _MONITORING_NS,
            "labels": {"app": _KSM_LABEL},
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": _KSM_LABEL}},
            "template": {
                "metadata": {"labels": {"app": _KSM_LABEL}},
                "spec": {
                    "serviceAccountName": _KSM_LABEL,
                    "containers": [
                        {
                            "name": _KSM_LABEL,
                            "image": image,
                            "ports": [
                                {
                                    "containerPort": _KSM_PORT,
                                    "name": "http-metrics",
                                },
                            ],
                            "readinessProbe": {
                                "httpGet": {
                                    "path": "/healthz",
                                    "port": _KSM_PORT,
                                },
                                "initialDelaySeconds": 5,
                                "timeoutSeconds": 5,
                            },
                            "resources": {
                                "requests": {
                                    "cpu": "50m",
                                    "memory": "64Mi",
                                },
                                "limits": {
                                    "cpu": "100m",
                                    "memory": "128Mi",
                                },
                            },
                        },
                    ],
                },
            },
        },
    }

    service: dict[str, Any] = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": _KSM_LABEL,
            "namespace": _MONITORING_NS,
            "labels": {"app": _KSM_LABEL},
        },
        "spec": {
            "type": "ClusterIP",
            "selector": {"app": _KSM_LABEL},
            "ports": [
                {
                    "port": _KSM_PORT,
                    "targetPort": _KSM_PORT,
                    "name": "http-metrics",
                },
            ],
        },
    }

    return [sa, cluster_role, crb, deployment, service]


# -- VictoriaMetrics ----------------------------------------------------------


def build_scrape_config() -> str:
    """Return a Prometheus-compatible scrape config for VictoriaMetrics."""
    return """\
global:
  scrape_interval: 15s
  scrape_timeout: 10s

scrape_configs:
  - job_name: "kube-state-metrics"
    static_configs:
      - targets:
          - "kube-state-metrics.monitoring.svc.cluster.local:8080"

  - job_name: "kubelet-cadvisor"
    scheme: https
    tls_config:
      insecure_skip_verify: true
    bearer_token_file: /var/run/secrets/kubernetes.io/serviceaccount/token
    kubernetes_sd_configs:
      - role: node
    relabel_configs:
      - action: labelmap
        regex: __meta_kubernetes_node_label_(.+)
    metrics_path: /metrics/cadvisor

  - job_name: "linkerd-proxy"
    kubernetes_sd_configs:
        - role: pod
    relabel_configs:
        - source_labels: [__meta_kubernetes_pod_container_name]
          action: keep
          regex: linkerd-proxy
        - source_labels: [__meta_kubernetes_pod_ip]
          action: replace
          target_label: __address__
          regex: (.+)
          replacement: "${1}:4191"
        - source_labels: [__meta_kubernetes_namespace]
          action: replace
          target_label: namespace
        - source_labels: [__meta_kubernetes_pod_name]
          action: replace
          target_label: pod
        - source_labels: [__meta_kubernetes_pod_label_app]
          action: replace
          target_label: deployment
"""


def build_victoria_metrics(
    image: str,
    nodeport: int,
) -> list[dict[str, Any]]:
    """Return all manifests needed for VictoriaMetrics single-node."""
    scrape_config = build_scrape_config()

    sa: dict[str, Any] = {
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {
            "name": _VM_LABEL,
            "namespace": _MONITORING_NS,
        },
    }

    cluster_role: dict[str, Any] = {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRole",
        "metadata": {"name": _VM_LABEL},
        "rules": [
            {
                "apiGroups": [""],
                "resources": [
                    "nodes",
                    "nodes/metrics",
                    "services",
                    "endpoints",
                    "pods",
                ],
                "verbs": ["get", "list", "watch"],
            },
            {
                "apiGroups": ["networking.k8s.io"],
                "resources": ["ingresses"],
                "verbs": ["get", "list", "watch"],
            },
            {
                "nonResourceURLs": ["/metrics", "/metrics/cadvisor"],
                "verbs": ["get"],
            },
        ],
    }

    crb: dict[str, Any] = {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRoleBinding",
        "metadata": {"name": _VM_LABEL},
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": _VM_LABEL,
        },
        "subjects": [
            {
                "kind": "ServiceAccount",
                "name": _VM_LABEL,
                "namespace": _MONITORING_NS,
            },
        ],
    }

    configmap: dict[str, Any] = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "vm-scrape-config",
            "namespace": _MONITORING_NS,
        },
        "data": {"scrape.yml": scrape_config},
    }

    deployment: dict[str, Any] = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": _VM_LABEL,
            "namespace": _MONITORING_NS,
            "labels": {"app": _VM_LABEL},
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": _VM_LABEL}},
            "template": {
                "metadata": {"labels": {"app": _VM_LABEL}},
                "spec": {
                    "serviceAccountName": _VM_LABEL,
                    "containers": [
                        {
                            "name": _VM_LABEL,
                            "image": image,
                            "args": [
                                "-promscrape.config=/config/scrape.yml",
                                "-retentionPeriod=7d",
                                "-httpListenAddr=:8428",
                            ],
                            "ports": [
                                {
                                    "containerPort": _VM_PORT,
                                    "name": "http",
                                },
                            ],
                            "readinessProbe": {
                                "httpGet": {
                                    "path": "/health",
                                    "port": _VM_PORT,
                                },
                                "initialDelaySeconds": 5,
                                "timeoutSeconds": 5,
                            },
                            "volumeMounts": [
                                {
                                    "name": "scrape-config",
                                    "mountPath": "/config",
                                    "readOnly": True,
                                },
                                {
                                    "name": "data",
                                    "mountPath": "/victoria-metrics-data",
                                },
                            ],
                            "resources": {
                                "requests": {
                                    "cpu": "100m",
                                    "memory": "128Mi",
                                },
                                "limits": {
                                    "cpu": "500m",
                                    "memory": "512Mi",
                                },
                            },
                        },
                    ],
                    "volumes": [
                        {
                            "name": "scrape-config",
                            "configMap": {"name": "vm-scrape-config"},
                        },
                        {
                            "name": "data",
                            "emptyDir": {},
                        },
                    ],
                },
            },
        },
    }

    service: dict[str, Any] = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": _VM_LABEL,
            "namespace": _MONITORING_NS,
            "labels": {"app": _VM_LABEL},
        },
        "spec": {
            "type": "NodePort",
            "selector": {"app": _VM_LABEL},
            "ports": [
                {
                    "port": _VM_PORT,
                    "targetPort": _VM_PORT,
                    "nodePort": nodeport,
                    "name": "http",
                },
            ],
        },
    }

    return [sa, cluster_role, crb, configmap, deployment, service]
