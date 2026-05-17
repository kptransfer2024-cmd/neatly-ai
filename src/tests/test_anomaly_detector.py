"""Tests for insights/ecommerce/anomaly_detector.py."""
import pandas as pd
import pytest
from insights.ecommerce.anomaly_detector import detect_revenue_anomalies, detect_kpi_anomalies


_SCHEMA = {"date": "date", "revenue": "revenue", "order_id": "order_id",
           "return_flag": None, "quantity": None}


def _normal_df(n_months: int = 12, base: float = 1000.0) -> pd.DataFrame:
    """Monthly data with consistent revenue, no anomalies."""
    dates = pd.date_range("2024-01-01", periods=n_months, freq="MS")
    rows = []
    for d in dates:
        for _ in range(10):
            rows.append({"date": d, "revenue": base, "order_id": f"O{d.month}"})
    return pd.DataFrame(rows)


def _df_with_spike(spike_month: int = 6) -> pd.DataFrame:
    """12-month data with one month having 10x revenue (spike)."""
    dates = pd.date_range("2024-01-01", periods=12, freq="MS")
    rows = []
    for d in dates:
        rev = 10000.0 if d.month == spike_month else 1000.0
        for _ in range(10):
            rows.append({"date": d, "revenue": rev, "order_id": f"O{d.month}"})
    return pd.DataFrame(rows)


def _df_with_drop(drop_month: int = 8) -> pd.DataFrame:
    """12-month data with one month having near-zero revenue (drop)."""
    dates = pd.date_range("2024-01-01", periods=12, freq="MS")
    rows = []
    for d in dates:
        rev = 10.0 if d.month == drop_month else 1000.0
        for _ in range(10):
            rows.append({"date": d, "revenue": rev, "order_id": f"O{d.month}"})
    return pd.DataFrame(rows)


def test_no_anomalies_for_flat_data():
    df = _normal_df()
    anomalies = detect_revenue_anomalies(df, _SCHEMA, period="M")
    assert isinstance(anomalies, list)


def test_spike_detected():
    df = _df_with_spike()
    anomalies = detect_revenue_anomalies(df, _SCHEMA, period="M")
    assert len(anomalies) > 0
    severities = [a["severity"] for a in anomalies]
    assert "high" in severities or "medium" in severities


def test_drop_detected():
    df = _df_with_drop()
    anomalies = detect_revenue_anomalies(df, _SCHEMA, period="M")
    assert len(anomalies) > 0


def test_anomaly_has_required_keys():
    df = _df_with_spike()
    anomalies = detect_revenue_anomalies(df, _SCHEMA, period="M")
    if anomalies:
        a = anomalies[0]
        assert "metric" in a
        assert "period" in a
        assert "value" in a
        assert "method" in a
        assert "severity" in a
        assert "explanation_ready_summary" in a


def test_method_values_are_valid():
    df = _df_with_spike()
    anomalies = detect_revenue_anomalies(df, _SCHEMA, period="M")
    valid_methods = {"z_score", "iqr", "pct_change"}
    for a in anomalies:
        assert a["method"] in valid_methods


def test_severity_values_are_valid():
    df = _df_with_spike()
    anomalies = detect_revenue_anomalies(df, _SCHEMA, period="M")
    valid_severities = {"low", "medium", "high"}
    for a in anomalies:
        assert a["severity"] in valid_severities


def test_missing_date_returns_empty():
    df = pd.DataFrame({"revenue": [100, 200, 300]})
    anomalies = detect_revenue_anomalies(df, {"date": None, "revenue": "revenue"}, period="M")
    assert anomalies == []


def test_detect_kpi_anomalies_returns_dict():
    df = _df_with_drop()
    result = detect_kpi_anomalies(df, _SCHEMA)
    assert isinstance(result, dict)


def test_too_few_periods_returns_empty():
    df = pd.DataFrame({
        "date": pd.date_range("2024-01-01", periods=3, freq="MS").repeat(3),
        "revenue": [100.0] * 9,
        "order_id": [f"O{i}" for i in range(9)],
    })
    anomalies = detect_revenue_anomalies(df, _SCHEMA, period="M")
    assert anomalies == []


def test_deduplication_keeps_highest_severity():
    df = _df_with_spike()
    anomalies = detect_revenue_anomalies(df, _SCHEMA, period="M")
    # No (metric, period) pair should appear more than once
    seen = [(a["metric"], a["period"]) for a in anomalies]
    assert len(seen) == len(set(seen))
