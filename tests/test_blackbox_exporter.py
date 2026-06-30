import yaml

from app.infrastructure.config_values import get_renderer

_TEST_IMAGE = "prom/blackbox-exporter:v0.25.0"
_VM_IMAGE = "victoriametrics/victoria-metrics:v1.108.1"
_VM_NODEPORT = 30090


def _blackbox_manifests() -> list[dict]:
    return get_renderer().render_to_dicts(
        "monitoring/blackbox-exporter.yaml.j2", bbe_image=_TEST_IMAGE
    )


def _scrape_config() -> dict:
    """The Prometheus scrape config now lives in the VM ConfigMap (scrape.yml)."""
    manifests = get_renderer().render_to_dicts(
        "monitoring/victoria-metrics.yaml.j2",
        vm_image=_VM_IMAGE,
        vm_nodeport=_VM_NODEPORT,
    )
    cm = next(m for m in manifests if m["kind"] == "ConfigMap")
    return yaml.safe_load(cm["data"]["scrape.yml"])


class TestBuildBlackboxExporter:
    def test_returns_three_manifests(self) -> None:
        assert len(_blackbox_manifests()) == 3

    def test_manifest_kinds(self) -> None:
        kinds = [m["kind"] for m in _blackbox_manifests()]
        assert kinds == ["ConfigMap", "Deployment", "Service"]

    def test_all_in_monitoring_namespace(self) -> None:
        for m in _blackbox_manifests():
            assert m["metadata"]["namespace"] == "monitoring"

    # -- ConfigMap -------------------------------------------------------

    def test_configmap_has_blackbox_yml(self) -> None:
        cm = _blackbox_manifests()[0]
        assert "blackbox.yml" in cm["data"]

    def test_configmap_contains_http_2xx_module(self) -> None:
        cm = _blackbox_manifests()[0]
        config = yaml.safe_load(cm["data"]["blackbox.yml"])
        assert "http_2xx" in config["modules"]
        assert config["modules"]["http_2xx"]["prober"] == "http"

    def test_configmap_contains_tcp_connect_module(self) -> None:
        cm = _blackbox_manifests()[0]
        config = yaml.safe_load(cm["data"]["blackbox.yml"])
        assert "tcp_connect" in config["modules"]
        assert config["modules"]["tcp_connect"]["prober"] == "tcp"

    def test_configmap_contains_grpc_module(self) -> None:
        cm = _blackbox_manifests()[0]
        config = yaml.safe_load(cm["data"]["blackbox.yml"])
        assert "grpc" in config["modules"]
        assert config["modules"]["grpc"]["prober"] == "grpc"

    # -- Deployment ------------------------------------------------------

    def test_deployment_uses_provided_image(self) -> None:
        container = _blackbox_manifests()[1]["spec"]["template"]["spec"]["containers"][0]
        assert container["image"] == _TEST_IMAGE

    def test_deployment_has_replicas(self) -> None:
        assert _blackbox_manifests()[1]["spec"]["replicas"] == 1

    def test_deployment_has_selector(self) -> None:
        dep = _blackbox_manifests()[1]
        assert dep["spec"]["selector"] == {"matchLabels": {"app": "blackbox-exporter"}}

    def test_deployment_has_readiness_probe(self) -> None:
        container = _blackbox_manifests()[1]["spec"]["template"]["spec"]["containers"][0]
        probe = container["readinessProbe"]
        assert probe["httpGet"]["path"] == "/-/healthy"
        assert probe["httpGet"]["port"] == 9115

    def test_deployment_has_resource_limits(self) -> None:
        container = _blackbox_manifests()[1]["spec"]["template"]["spec"]["containers"][0]
        assert "requests" in container["resources"]
        assert "limits" in container["resources"]

    def test_deployment_mounts_config_volume(self) -> None:
        container = _blackbox_manifests()[1]["spec"]["template"]["spec"]["containers"][0]
        mount = container["volumeMounts"][0]
        assert mount["mountPath"] == "/config"
        assert mount["readOnly"] is True

    def test_deployment_volume_references_configmap(self) -> None:
        manifests = _blackbox_manifests()
        cm_name = manifests[0]["metadata"]["name"]
        volumes = manifests[1]["spec"]["template"]["spec"]["volumes"]
        assert any(v.get("configMap", {}).get("name") == cm_name for v in volumes)

    def test_deployment_container_port(self) -> None:
        container = _blackbox_manifests()[1]["spec"]["template"]["spec"]["containers"][0]
        assert container["ports"][0]["containerPort"] == 9115

    def test_deployment_has_app_label(self) -> None:
        assert _blackbox_manifests()[1]["metadata"]["labels"]["app"] == "blackbox-exporter"

    # -- Service ---------------------------------------------------------

    def test_service_is_clusterip(self) -> None:
        assert _blackbox_manifests()[2]["spec"]["type"] == "ClusterIP"

    def test_service_port(self) -> None:
        port = _blackbox_manifests()[2]["spec"]["ports"][0]
        assert port["port"] == 9115
        assert port["targetPort"] == 9115

    def test_service_selector(self) -> None:
        assert _blackbox_manifests()[2]["spec"]["selector"] == {"app": "blackbox-exporter"}


class TestScrapeConfigBlackbox:
    def test_scrape_config_parses_as_valid_yaml(self) -> None:
        assert "scrape_configs" in _scrape_config()

    def test_contains_blackbox_http_job(self) -> None:
        job_names = [j["job_name"] for j in _scrape_config()["scrape_configs"]]
        assert "blackbox-http" in job_names

    def test_contains_blackbox_tcp_job(self) -> None:
        job_names = [j["job_name"] for j in _scrape_config()["scrape_configs"]]
        assert "blackbox-tcp" in job_names

    def test_contains_blackbox_grpc_job(self) -> None:
        job_names = [j["job_name"] for j in _scrape_config()["scrape_configs"]]
        assert "blackbox-grpc" in job_names

    def test_blackbox_grpc_uses_grpc_module(self) -> None:
        job = next(
            j for j in _scrape_config()["scrape_configs"] if j["job_name"] == "blackbox-grpc"
        )
        assert job["params"]["module"] == ["grpc"]

    def test_blackbox_grpc_keeps_only_grpc_port_names(self) -> None:
        job = next(
            j for j in _scrape_config()["scrape_configs"] if j["job_name"] == "blackbox-grpc"
        )
        keep_rules = [r for r in job["relabel_configs"] if r.get("action") == "keep"]
        assert len(keep_rules) >= 1
        assert any(
            "service_port_name" in (r.get("source_labels", [None])[0] or "")
            for r in keep_rules
        )

    def test_blackbox_http_uses_http_2xx_module(self) -> None:
        job = next(
            j for j in _scrape_config()["scrape_configs"] if j["job_name"] == "blackbox-http"
        )
        assert job["params"]["module"] == ["http_2xx"]

    def test_blackbox_tcp_uses_tcp_connect_module(self) -> None:
        job = next(
            j for j in _scrape_config()["scrape_configs"] if j["job_name"] == "blackbox-tcp"
        )
        assert job["params"]["module"] == ["tcp_connect"]

    def test_blackbox_jobs_route_through_exporter(self) -> None:
        config = _scrape_config()
        for job_name in ("blackbox-http", "blackbox-tcp"):
            job = next(j for j in config["scrape_configs"] if j["job_name"] == job_name)
            replacements = [
                r.get("replacement") for r in job["relabel_configs"] if "replacement" in r
            ]
            assert any(
                "blackbox-exporter.monitoring.svc.cluster.local:9115" in r
                for r in replacements
            )

    def test_blackbox_jobs_drop_system_namespaces(self) -> None:
        config = _scrape_config()
        for job_name in ("blackbox-http", "blackbox-tcp"):
            job = next(j for j in config["scrape_configs"] if j["job_name"] == job_name)
            drop_rules = [r for r in job["relabel_configs"] if r.get("action") == "drop"]
            assert len(drop_rules) >= 1
            assert "kube-system" in drop_rules[0]["regex"]

    def test_blackbox_jobs_use_service_discovery(self) -> None:
        config = _scrape_config()
        for job_name in ("blackbox-http", "blackbox-tcp"):
            job = next(j for j in config["scrape_configs"] if j["job_name"] == job_name)
            assert job["kubernetes_sd_configs"][0]["role"] == "service"


class TestVictoriaMetricsIncludesBlackboxScrape:
    def test_vm_configmap_contains_blackbox_jobs(self) -> None:
        manifests = get_renderer().render_to_dicts(
            "monitoring/victoria-metrics.yaml.j2", vm_image=_VM_IMAGE, vm_nodeport=_VM_NODEPORT
        )
        cm = next(m for m in manifests if m["kind"] == "ConfigMap")
        scrape_yml = cm["data"]["scrape.yml"]
        assert "blackbox-http" in scrape_yml
        assert "blackbox-tcp" in scrape_yml
