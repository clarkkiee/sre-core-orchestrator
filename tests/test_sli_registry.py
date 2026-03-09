from app.infrastructure.metrics.sli_registry import (
    SLI_REGISTRY,
    evaluate_slo,
    get_all_sub_characteristics,
    get_slis_for_sub_characteristics,
    render_promql,
)


class TestRenderPromql:
    def test_substitutes_namespace(self) -> None:
        sli = SLI_REGISTRY["AV-1"]
        result = render_promql(sli, namespace="my-app")
        assert 'namespace="my-app"' in result
        assert "$ns" not in result

    def test_substitutes_window(self) -> None:
        sli = SLI_REGISTRY["FT-1"]
        result = render_promql(sli, namespace="default", window="10m")
        assert "[10m]" in result
        assert "$window" not in result

    def test_default_window(self) -> None:
        sli = SLI_REGISTRY["MA-1"]
        result = render_promql(sli, namespace="default")
        assert "[5m]" in result


class TestEvaluateSlo:
    def test_gte_met(self) -> None:
        sli = SLI_REGISTRY["AV-1"]  # slo_target=0.99, operator=gte
        assert evaluate_slo(0.995, sli) is True

    def test_gte_not_met(self) -> None:
        sli = SLI_REGISTRY["AV-1"]
        assert evaluate_slo(0.50, sli) is False

    def test_gte_exact_boundary(self) -> None:
        sli = SLI_REGISTRY["AV-1"]
        assert evaluate_slo(0.99, sli) is True

    def test_lte_met(self) -> None:
        sli = SLI_REGISTRY["FT-1"]  # slo_target=0.80, operator=lte
        assert evaluate_slo(0.50, sli) is True

    def test_lte_not_met(self) -> None:
        sli = SLI_REGISTRY["FT-1"]
        assert evaluate_slo(0.90, sli) is False

    def test_nonen_value(self) -> None:
        sli = SLI_REGISTRY["AV-1"]
        assert evaluate_slo(None, sli) is None


class TestRegistry:
    def test_has_four_slis(self) -> None:
        assert len(SLI_REGISTRY) == 4

    def test_all_sub_characteristics_covered(self) -> None:
        subs = get_all_sub_characteristics()
        assert set(subs) == {
            "availability",
            "fault_tolerance",
            "faultlessness",
            "recoverability",
        }

    def test_get_slis_for_sub_characteristics(self) -> None:
        av_slis = get_slis_for_sub_characteristics("availability")
        assert len(av_slis) == 1
        assert av_slis[0].id == "AV-1"

    def test_each_sli_has_required_fields(self) -> None:
        for sli in SLI_REGISTRY.values():
            assert sli.id
            assert sli.name
            assert sli.sub_characteristics
            assert sli.promql_template
            assert sli.unit
            assert sli.slo_operator in ("gte", "lte", "eq")
