"""Tests for insights/ecommerce/insight_reporter.py — including privacy guard."""
import pytest
from insights.ecommerce.insight_reporter import (
    build_insight_payload,
    validate_no_raw_rows_in_insight_payload,
    generate_fallback_summary,
)
from utils.privacy_guards import validate_no_raw_rows


_GOOD_KPIS = {"total_revenue": 50000.0, "total_orders": 200, "average_order_value": 250.0}
_GOOD_EDA = {"row_count": 500, "missing_pct": 2.5}
_GOOD_TRENDS = {"revenue_trend": {"trend_label": "improving", "notes": []}}
_GOOD_ANOMALIES = [{"metric": "revenue", "period": "2024-08", "value": 100.0, "method": "z_score", "severity": "high", "explanation_ready_summary": "Low revenue in August."}]
_GOOD_DRIVERS = {"overall_change": {"change": -5000.0, "change_pct": -10.0, "previous_period": "2024-07", "latest_period": "2024-08"}}


# ---------------------------------------------------------------------------
# Privacy guard — validate_no_raw_rows_in_insight_payload
# ---------------------------------------------------------------------------

def test_valid_payload_passes():
    payload = build_insight_payload(_GOOD_KPIS, _GOOD_EDA, _GOOD_TRENDS, _GOOD_ANOMALIES, _GOOD_DRIVERS)
    assert isinstance(payload, dict)


def test_blocked_key_row_indices_raises():
    bad_payload = {"kpis": {"total_revenue": 100}, "row_indices": [0, 1, 2, 3, 4]}
    with pytest.raises(ValueError, match="blocked key"):
        validate_no_raw_rows_in_insight_payload(bad_payload)


def test_nested_blocked_key_raises():
    bad_payload = {"kpis": {"detail": {"row_indices": list(range(200))}}}
    with pytest.raises(ValueError, match="blocked key"):
        validate_no_raw_rows_in_insight_payload(bad_payload)


def test_large_list_of_dicts_raises():
    bad_payload = {"rows": [{"a": i} for i in range(600)]}
    with pytest.raises(ValueError, match="large list"):
        validate_no_raw_rows_in_insight_payload(bad_payload)


def test_small_list_of_dicts_passes():
    good_payload = {"top_products": [{"product": f"P{i}", "revenue": i * 100} for i in range(10)]}
    validate_no_raw_rows_in_insight_payload(good_payload)


def test_privacy_guards_module_blocked_key():
    with pytest.raises(ValueError, match="blocked key"):
        validate_no_raw_rows({"row_indices": [0, 1, 2]}, "test_context")


def test_privacy_guards_module_large_list():
    with pytest.raises(ValueError):
        validate_no_raw_rows([{"x": i} for i in range(600)], "test_context")


def test_privacy_guards_module_passes_for_aggregates():
    validate_no_raw_rows({"total_revenue": 50000, "top_cats": [{"cat": "A", "revenue": 1000}]}, "test")


# ---------------------------------------------------------------------------
# Fallback summary
# ---------------------------------------------------------------------------

def test_fallback_summary_includes_key_metrics():
    payload = build_insight_payload(_GOOD_KPIS, _GOOD_EDA, _GOOD_TRENDS, _GOOD_ANOMALIES, _GOOD_DRIVERS)
    summary = generate_fallback_summary(payload)
    assert "Total Revenue" in summary or "revenue" in summary.lower()


def test_fallback_summary_includes_trend():
    payload = build_insight_payload(_GOOD_KPIS, _GOOD_EDA, _GOOD_TRENDS, _GOOD_ANOMALIES, _GOOD_DRIVERS)
    summary = generate_fallback_summary(payload)
    assert "improving" in summary.lower() or "trend" in summary.lower()


def test_fallback_summary_includes_anomaly_section():
    payload = build_insight_payload(_GOOD_KPIS, _GOOD_EDA, _GOOD_TRENDS, _GOOD_ANOMALIES, _GOOD_DRIVERS)
    summary = generate_fallback_summary(payload)
    assert "Anomal" in summary


def test_fallback_summary_includes_limitations():
    payload = build_insight_payload(_GOOD_KPIS, _GOOD_EDA, _GOOD_TRENDS, _GOOD_ANOMALIES, _GOOD_DRIVERS)
    summary = generate_fallback_summary(payload)
    assert "Limitation" in summary


def test_fallback_summary_uses_hedged_language():
    payload = build_insight_payload(_GOOD_KPIS, _GOOD_EDA, _GOOD_TRENDS, _GOOD_ANOMALIES, _GOOD_DRIVERS)
    summary = generate_fallback_summary(payload)
    hedges = ["suggests", "appears", "may indicate", "should be investigated", "likely", "seems"]
    assert any(h in summary.lower() for h in hedges)


def test_fallback_returns_string():
    payload = build_insight_payload(_GOOD_KPIS, _GOOD_EDA, _GOOD_TRENDS, [], {})
    result = generate_fallback_summary(payload)
    assert isinstance(result, str)
    assert len(result) > 50


def test_build_insight_payload_validates_on_construction():
    """build_insight_payload must reject payloads containing blocked keys."""
    bad_kpis = {"total_revenue": 100, "row_indices": [0, 1, 2, 3]}
    with pytest.raises(ValueError, match="blocked key"):
        build_insight_payload(bad_kpis, {}, {}, [], {})


# ---------------------------------------------------------------------------
# Static drivers and generic EDA in payload
# ---------------------------------------------------------------------------

_STATIC_DRIVERS = {
    "static_by_category": [
        {"value": "Electronics", "count": 10, "total_revenue": 5000.0, "return_rate": 0.1},
        {"value": "Furniture", "count": 5, "total_revenue": 2000.0, "return_rate": 0.2},
    ],
    "dimensions_analyzed": ["category"],
    "concentration_warning": None,
    "mode": "static",
}

_GENERIC_EDA = {
    "overall_return_rate": 0.12,
    "overall_churn_rate": 0.08,
    "numeric_correlations": [{"col_a": "revenue", "col_b": "quantity", "correlation": 0.75}],
}


def test_payload_includes_static_drivers():
    payload = build_insight_payload(
        _GOOD_KPIS, _GOOD_EDA, _GOOD_TRENDS, _GOOD_ANOMALIES, _GOOD_DRIVERS,
        static_drivers=_STATIC_DRIVERS,
    )
    assert "static_drivers" in payload
    assert payload["static_drivers"]["mode"] == "static"


def test_payload_includes_generic_eda():
    payload = build_insight_payload(
        _GOOD_KPIS, _GOOD_EDA, _GOOD_TRENDS, _GOOD_ANOMALIES, _GOOD_DRIVERS,
        generic_eda=_GENERIC_EDA,
    )
    assert "generic_eda" in payload
    assert "overall_return_rate" in payload["generic_eda"]


def test_fallback_summary_includes_static_drivers_section():
    payload = build_insight_payload(
        _GOOD_KPIS, _GOOD_EDA, _GOOD_TRENDS, [], _GOOD_DRIVERS,
        static_drivers=_STATIC_DRIVERS,
    )
    summary = generate_fallback_summary(payload)
    assert "Segment" in summary or "Electronics" in summary


def test_fallback_summary_includes_generic_eda_section():
    payload = build_insight_payload(
        _GOOD_KPIS, _GOOD_EDA, _GOOD_TRENDS, [], {},
        generic_eda=_GENERIC_EDA,
    )
    summary = generate_fallback_summary(payload)
    assert "return rate" in summary.lower() or "churn" in summary.lower()


def test_fallback_no_dollar_signs():
    payload = build_insight_payload(_GOOD_KPIS, _GOOD_EDA, _GOOD_TRENDS, _GOOD_ANOMALIES, _GOOD_DRIVERS)
    summary = generate_fallback_summary(payload)
    assert "$" not in summary, "Dollar signs must not appear in fallback summary"


def test_revenue_provenance_in_limitations():
    kpis_with_source = {**_GOOD_KPIS, "revenue_source_column": "Transaction Value", "revenue_label": "Total Transaction Value"}
    payload = build_insight_payload(kpis_with_source, _GOOD_EDA, _GOOD_TRENDS, [], {})
    summary = generate_fallback_summary(payload)
    assert "Transaction Value" in summary
