"""Tests for insights/ecommerce/driver_analyzer.py."""
import pandas as pd
import pytest
from insights.ecommerce.driver_analyzer import (
    analyze_revenue_drivers,
    decompose_revenue_change_by_dimension,
    analyze_static_drivers,
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


# ---------------------------------------------------------------------------
# Static driver analysis (no date required)
# ---------------------------------------------------------------------------

_STATIC_SCHEMA = {
    "date": None,
    "revenue": "revenue",
    "order_id": None,
    "customer_id": None,
    "product": "product",
    "category": "category",
    "quantity": None,
    "discount": None,
    "return_flag": "returned",
    "churn_flag": None,
    "region": None,
    "channel": None,
    "payment_method": None,
    "price": None,
    "customer_name": None,
}


def _static_df() -> pd.DataFrame:
    return pd.DataFrame({
        "revenue": [500.0, 300.0, 200.0, 100.0, 400.0, 600.0],
        "category": ["Electronics", "Furniture", "Electronics", "Furniture", "Electronics", "Electronics"],
        "product": ["A", "B", "A", "B", "C", "A"],
        "returned": ["No", "Yes", "No", "No", "No", "Yes"],
    })


def test_static_drivers_returns_dict():
    df = _static_df()
    result = analyze_static_drivers(df, _STATIC_SCHEMA)
    assert isinstance(result, dict)


def test_static_drivers_no_date_required():
    df = _static_df()
    result = analyze_static_drivers(df, _STATIC_SCHEMA)
    assert "error" not in result


def test_static_drivers_includes_dimensions_analyzed():
    df = _static_df()
    result = analyze_static_drivers(df, _STATIC_SCHEMA)
    assert "dimensions_analyzed" in result
    assert "category" in result["dimensions_analyzed"]


def test_static_drivers_sorted_by_revenue_desc():
    df = _static_df()
    result = analyze_static_drivers(df, _STATIC_SCHEMA)
    cats = result.get("static_by_category", [])
    assert len(cats) >= 2
    assert cats[0]["total_revenue"] >= cats[1]["total_revenue"]


def test_static_drivers_top_category_is_electronics():
    df = _static_df()
    result = analyze_static_drivers(df, _STATIC_SCHEMA)
    cats = result.get("static_by_category", [])
    assert cats[0]["value"] == "Electronics"


def test_static_drivers_includes_return_rate():
    df = _static_df()
    result = analyze_static_drivers(df, _STATIC_SCHEMA)
    cats = result.get("static_by_category", [])
    assert "return_rate" in cats[0]


def test_static_drivers_concentration_warning_high():
    df = pd.DataFrame({
        "revenue": [900.0, 50.0, 50.0],
        "category": ["Dominant", "Other", "Other"],
    })
    schema = {**_STATIC_SCHEMA, "category": "category"}
    result = analyze_static_drivers(df, schema)
    assert result.get("concentration_warning") is not None


def test_static_drivers_no_concentration_warning_when_even():
    df = pd.DataFrame({
        "revenue": [100.0, 100.0, 100.0],
        "category": ["A", "B", "C"],
    })
    schema = {**_STATIC_SCHEMA, "category": "category"}
    result = analyze_static_drivers(df, schema)
    assert not result.get("concentration_warning")


def test_static_drivers_mode_field():
    df = _static_df()
    result = analyze_static_drivers(df, _STATIC_SCHEMA)
    assert result.get("mode") == "static"


def test_static_drivers_no_revenue_no_qty_returns_error():
    df = pd.DataFrame({"category": ["A", "B"]})
    schema = {**_STATIC_SCHEMA, "revenue": None, "quantity": None}
    result = analyze_static_drivers(df, schema)
    assert "error" in result


def test_static_drivers_empty_dataframe():
    df = pd.DataFrame(columns=["revenue", "category"])
    result = analyze_static_drivers(df, _STATIC_SCHEMA)
    assert isinstance(result, dict)
