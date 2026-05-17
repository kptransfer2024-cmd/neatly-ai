"""Tests for insights/ecommerce/trend_analyzer.py."""
import pandas as pd
import pytest
from insights.ecommerce.trend_analyzer import analyze_revenue_trend, analyze_kpi_trends


_SCHEMA = {"date": "date", "revenue": "revenue", "order_id": "order_id", "quantity": "quantity"}


def _trend_df(revenues: list[float], start: str = "2024-01-01") -> pd.DataFrame:
    """Build a monthly DataFrame with given revenue sequence."""
    dates = pd.date_range(start, periods=len(revenues), freq="MS")
    rows = []
    for d, r in zip(dates, revenues):
        rows.append({"date": d, "revenue": r, "order_id": f"O{d.month}", "quantity": 1})
    return pd.DataFrame(rows)


def test_increasing_trend_labeled_improving():
    df = _trend_df([100, 150, 200, 250, 310])
    result = analyze_revenue_trend(df, _SCHEMA)
    assert result["trend_label"] == "improving"


def test_decreasing_trend_labeled_declining():
    df = _trend_df([500, 400, 300, 200, 100])
    result = analyze_revenue_trend(df, _SCHEMA)
    assert result["trend_label"] == "declining"


def test_flat_trend_labeled_flat():
    df = _trend_df([100, 102, 99, 101, 100])
    result = analyze_revenue_trend(df, _SCHEMA)
    assert result["trend_label"] == "flat"


def test_best_and_worst_period_populated():
    df = _trend_df([100, 500, 50])
    result = analyze_revenue_trend(df, _SCHEMA)
    assert result["best_period"] is not None
    assert result["worst_period"] is not None


def test_pct_changes_first_is_none():
    df = _trend_df([100, 200, 300])
    result = analyze_revenue_trend(df, _SCHEMA)
    assert result["pct_changes"][0] is None


def test_largest_drop_detected():
    df = _trend_df([300, 100, 200, 150])
    result = analyze_revenue_trend(df, _SCHEMA)
    drop = result.get("largest_drop")
    assert drop is not None
    assert drop["pct_change"] < 0


def test_rolling_avg_returned_for_4_plus_periods():
    df = _trend_df([100, 120, 110, 130, 140])
    result = analyze_revenue_trend(df, _SCHEMA)
    rolling = result.get("rolling_avg", [])
    assert len(rolling) == len(result["periods"])


def test_notes_is_list():
    df = _trend_df([100, 200, 150, 250])
    result = analyze_revenue_trend(df, _SCHEMA)
    assert isinstance(result["notes"], list)


def test_missing_date_column_returns_error():
    df = pd.DataFrame({"revenue": [100, 200]})
    result = analyze_revenue_trend(df, {"date": "date", "revenue": "revenue"})
    assert "error" in result


def test_kpi_trends_includes_revenue_trend():
    df = _trend_df([100, 150, 200])
    result = analyze_kpi_trends(df, _SCHEMA)
    assert "revenue_trend" in result


def test_analyze_kpi_trends_includes_order_count():
    df = _trend_df([100, 150, 200])
    result = analyze_kpi_trends(df, _SCHEMA)
    assert "order_count_trend" in result


def test_empty_dataframe_does_not_crash():
    df = pd.DataFrame(columns=["date", "revenue"])
    result = analyze_revenue_trend(df, _SCHEMA)
    assert "error" in result or "periods" in result
