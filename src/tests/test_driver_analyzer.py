"""Tests for insights/ecommerce/driver_analyzer.py."""
import pandas as pd
import pytest
from insights.ecommerce.driver_analyzer import (
    analyze_revenue_drivers,
    decompose_revenue_change_by_dimension,
)


_SCHEMA = {
    "date": "date", "revenue": "revenue", "order_id": "order_id",
    "customer_id": None, "product": "product", "category": "category",
    "quantity": "quantity", "discount": None, "return_flag": None,
    "region": "region", "channel": "channel",
}


def _multi_period_df() -> pd.DataFrame:
    """Two completed monthly periods where Furniture causes a decline."""
    rows = []
    jan = pd.Timestamp("2024-01-01")
    feb = pd.Timestamp("2024-02-01")

    for _ in range(20):
        rows.append({"date": jan, "revenue": 1000.0, "category": "Electronics", "product": "A", "quantity": 1, "region": "North", "channel": "online", "order_id": "O1"})
        rows.append({"date": jan, "revenue": 800.0, "category": "Furniture", "product": "B", "quantity": 1, "region": "South", "channel": "retail", "order_id": "O2"})
        rows.append({"date": feb, "revenue": 1000.0, "category": "Electronics", "product": "A", "quantity": 1, "region": "North", "channel": "online", "order_id": "O3"})
        rows.append({"date": feb, "revenue": 200.0, "category": "Furniture", "product": "B", "quantity": 1, "region": "South", "channel": "retail", "order_id": "O4"})

    return pd.DataFrame(rows)


def test_overall_change_shows_decline():
    df = _multi_period_df()
    result = analyze_revenue_drivers(df, _SCHEMA)
    overall = result.get("overall_change", {})
    assert "error" not in overall
    assert overall["change"] < 0


def test_furniture_is_top_negative_driver():
    df = _multi_period_df()
    result = decompose_revenue_change_by_dimension(df, _SCHEMA, "category")
    drivers = result.get("drivers", [])
    assert len(drivers) > 0
    assert drivers[0]["dimension_value"] == "Furniture"
    assert drivers[0]["absolute_change"] < 0


def test_decompose_returns_required_keys():
    df = _multi_period_df()
    result = decompose_revenue_change_by_dimension(df, _SCHEMA, "category")
    assert "dimension" in result
    assert "drivers" in result
    assert "total_revenue_change" in result
    assert "previous_period" in result
    assert "latest_period" in result


def test_contribution_pct_sums_approximately_100():
    df = _multi_period_df()
    result = decompose_revenue_change_by_dimension(df, _SCHEMA, "category")
    total = sum(d["contribution_to_total_change_pct"] for d in result["drivers"])
    # Contributions sum to ±100% depending on direction of total change
    assert abs(abs(total) - 100.0) < 5.0


def test_all_dimension_drivers_in_result():
    df = _multi_period_df()
    result = analyze_revenue_drivers(df, _SCHEMA)
    assert "drivers_by_category" in result


def test_mechanism_summary_computed():
    df = _multi_period_df()
    result = analyze_revenue_drivers(df, _SCHEMA)
    assert "mechanism_summary" in result


def test_missing_revenue_returns_error():
    df = pd.DataFrame({"date": pd.date_range("2024-01-01", periods=3, freq="MS"), "revenue": [100, 200, 300]})
    schema = {**_SCHEMA, "revenue": None}
    result = analyze_revenue_drivers(df, schema)
    assert "error" in result


def test_insufficient_periods_returns_error():
    df = pd.DataFrame({
        "date": [pd.Timestamp("2024-01-15")] * 5,
        "revenue": [100.0] * 5, "category": ["A"] * 5,
        "product": ["X"] * 5, "quantity": [1] * 5, "region": ["N"] * 5, "channel": ["o"] * 5, "order_id": ["O1"] * 5,
    })
    result = decompose_revenue_change_by_dimension(df, _SCHEMA, "category")
    assert "error" in result


def test_empty_dataframe_does_not_crash():
    df = pd.DataFrame(columns=["date", "revenue", "category", "product", "quantity", "region", "channel", "order_id"])
    result = analyze_revenue_drivers(df, _SCHEMA)
    assert isinstance(result, dict)
