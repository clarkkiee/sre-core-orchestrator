from dataclasses import dataclass


@dataclass(frozen=True)
class SLIDefinition:
    id: str
    name: str
    sub_characteristics: str
    promql_template: str
    unit: str
    slo_target: float
    slo_operator: str
    description: str


def render_promql(sli: SLIDefinition, namespace: str, window: str = "5m") -> str:
    return sli.promql_template.replace("$ns", namespace).replace("$window", window)


def evaluate_slo(value: float | None, sli: SLIDefinition) -> bool | None:
    if value is None:
        return None

    if sli.slo_operator == "gte":
        return value >= sli.slo_target
    if sli.slo_operator == "lte":
        return value <= sli.slo_target
    if sli.slo_operator == "eq":
        return value == sli.slo_target
    return None


SLI_REGISTRY: dict[str, SLIDefinition] = {
    # AVAILABILITY
    "AV-1": SLIDefinition(
        id="AV-1",
        name="Uptime Ratio",
        sub_characteristics="availability",
        promql_template=(
            'avg(kube_pod_status_ready{namespace="$ns", condition="true"})'
        ),
        unit="ratio",
        slo_target=0.99,
        slo_operator="gte",
        description="Fraction of pods in Ready state, representing application uptime",
    ),
    # FAULT TOLERANCE
    "FT-1": SLIDefinition(
        id="FT-1",
        name="Performance Degradation",
        sub_characteristics="fault_tolerance",
        promql_template=(
            "avg(rate(container_cpu_usage_seconds_total{"
            'namespace="$ns"'
            "}[$window]))"
            " / "
            "avg(kube_pod_container_resource_limits{"
            'namespace="$ns", resource="cpu"'
            "})"
        ),
        unit="ratio",
        slo_target=0.80,
        slo_operator="lte",
        description=(
            "CPU usage ratio relative to limits during fault injection. "
            "Values closer to 1.0 indicate performance degradation."
        ),
    ),
    # --- FAULTLESSNESS (MATURITY) ---
    "MA-1": SLIDefinition(
        id="MA-1",
        name="Error Rate",
        sub_characteristics="faultlessness",
        promql_template=(
            "sum(rate(kube_pod_container_status_restarts_total{"
            'namespace="$ns"'
            "}[$window]))"
            " + "
            "(count(kube_pod_status_phase{"
            'namespace="$ns", phase="Failed"'
            "}) or vector(0))"
            " / "
            "count(kube_pod_info{"
            'namespace="$ns"'
            "})"
        ),
        unit="ratio",
        slo_target=0.01,
        slo_operator="lte",
        description=(
            "Combined error signal from container restart rate and pod failure ratio. "
            "Lower values indicate higher faultlessness under normal operation."
        ),
    ),
    # --- RECOVERABILITY ---
    "RC-1": SLIDefinition(
        id="RC-1",
        name="MTTR",
        sub_characteristics="recoverability",
        promql_template=(
            'avg(kube_pod_status_ready{namespace="$ns", condition="true"})'
        ),
        unit="ratio",
        slo_target=0.99,
        slo_operator="gte",
        description=(
            "Pod readiness ratio measured during recovery window. "
            "MTTR is computed by tracking how long this value takes to return to SLO "
            "after fault injection ends."
        ),
    ),
}


def get_slis_for_sub_characteristics(sub: str) -> list[SLIDefinition]:
    return [s for s in SLI_REGISTRY.values() if s.sub_characteristics == sub]


def get_all_sub_characteristics() -> list[str]:
    return list(dict.fromkeys(s.sub_characteristics for s in SLI_REGISTRY.values()))
