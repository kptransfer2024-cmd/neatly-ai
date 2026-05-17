"""Tests for insights/ecommerce/kpi_calculator.py."""
import pandas as pd
import pytest
from insights.ecommerce.kpi_calculator import calculate_kpis, calculate_period_kpis


_SCHEMA = {
    "date": "date", "revenue": "revenue", "order_id": "order_id",
    "customer_id": "customer_id", "product": "product", "category": "category",
    "quantity": "quantity", "discount": "discount", "return_flag": None,
    "region": "region", "channel": "channel",
}


def _base_df():
    return pd.DataFrame({
        "date": pd.to_datetime(["2024-01-15", "2024-02-10", "2024-02-20", "2024-03-05"]),
        "revenue": [100.0, 200.0, 150.0, 300.0],
        "order_id": ["O1", "O2", "O3", "O4"],
        "customer_id": ["C1", "C2", "C1", "C3"],
        "product": ["Widget", "Gadget", "Widget", "Doohickey"],
        "category": ["Electronics", "Electronics", "Electronics", "Books"],
        "quantity": [1, 2, 1, 3],
        "discount": [0, 10.0, 5.0, 0],
        "region": ["North", "South", "North", "East"],
        "channel": ["online", "retail", "online", "online"],
    })


def test_total_revenue():
    df = _base_df()
    kpis = calculate_kpis(df, _SCHEMA)
    assert kpis["total_revenue"] == pytest.approx(750.0)


def test_total_orders():
    df = _base_df()
    kpis = calculate_kpis(df, _SCHEMA)
    assert kpis["total_orders"] == 4


def test_unique_customers():
    df = _base_df()
    kpis = calculate_kpis(df, _SCHEMA)
    assert kpis["unique_customers"] == 3


def test_average_order_value():
    df = _base_df()
    kpis = calculate_kpis(df, _SCHEMA)
    assert kpis["average_order_value"] == pytest.approx(187.5)


def test_total_units_sold():
    df = _base_df()
    kpis = calculate_kpis(df, _SCHEMA)
    assert kpis["total_units_sold"] == pytest.approx(7.0)


def test_revenue_per_customer():
    df = _base_df()
    kpis = calculate_kpis(df, _SCHEMA)
    assert kpis["revenue_per_customer"] == pytest.approx(250.0)


def test_top_categories_by_revenue():
    df = _base_df()
    kpis = calculate_kpis(df, _SCHEMA)
    cats = kpis["top_categories_by_revenue"]
    assert len(cats) > 0
    assert cats[0]["category"] == "Electronics"


def test_missing_revenue_column_skips():
    df = pd.DataFrame({"date": ["2024-01-01"], "order_id": ["O1"]})
    schema = {**_SCHEMA, "revenue": None}
    kpis = calculate_kpis(df, schema)
    assert "total_revenue" not in kpis


def test_minimal_schema_does_not_crash():
    df = pd.DataFrame({"revenue": [100.0, 200.0]})
    minimal = {k: None for k in _SCHEMA}
    minimal["revenue"] = "revenue"
    kpis = calculate_kpis(df, minimal)
    assert "total_revenue" in kpis


def test_period_kpis_returns_revenue_by_period():
    df = _base_df()
    result = calculate_period_kpis(df, _SCHEMA)
    assert "revenue_by_period" in result
    periods = [r["period"] for r in result["revenue_by_period"]]
    assert "2024-01" in periods
    assert "2024-02" in periods


def test_period_kpis_missing_date_returns_empty():
    df = _base_df()
    schema = {**_SCHEMA, "date": None}
    result = calculate_period_kpis(df, schema)
    assert result == {}


def test_empty_df_does_not_crash():
    df = pd.DataFrame(columns=["date", "revenue", "order_id"])
    kpis = calculate_kpis(df, _SCHEMA)
    assert isinstance(kpis, dict)


# ---------------------------------------------------------------------------
# New tests: Transaction Value column, labels, churn, payment methods
# ---------------------------------------------------------------------------

def test_revenue_label_and_source_column():
    df = pd.DataFrame({
        "Transaction Value": [100.0, 200.0],
        "order_id": ["O1", "O2"],
    })
    schema = {k: None for k in _SCHEMA}
    schema["revenue"] = "Transaction Value"
    schema["order_id"] = "order_id"
    kpis = calculate_kpis(df, schema)
    assert "revenue_label" in kpis
    assert "revenue_source_column" in kpis
    assert kpis["revenue_source_column"] == "Transaction Value"
    assert "transaction" in kpis["revenue_label"].lower() or "revenue" in kpis["revenue_label"].lower()


def test_transaction_value_computes_correctly():
    df = pd.DataFrame({
        "Transaction Value": [150.0, 250.0, 100.0],
        "order_id": ["O1", "O2", "O3"],
    })
    schema = {k: None for k in _SCHEMA}
    schema["revenue"] = "Transaction Value"
    schema["order_id"] = "order_id"
    kpis = calculate_kpis(df, schema)
    assert kpis["total_revenue"] == pytest.approx(500.0)


def test_churn_rate_computed():
    df = _base_df().copy()
    df["churn_status"] = ["Churned", "Active", "Churned", "Active"]
    schema = {**_SCHEMA, "churn_flag": "churn_status"}
    kpis = calculate_kpis(df, schema)
    assert "churn_rate" in kpis
    assert kpis["churn_rate"] == pytest.approx(0.5)


def test_churn_rate_missing_column_skips():
    df = _base_df()
    schema = {**_SCHEMA, "churn_flag": "nonexistent"}
    kpis = calculate_kpis(df, schema)
    assert "churn_rate" not in kpis


def test_top_payment_methods_by_revenue():
    df = _base_df().copy()
    df["payment"] = ["Credit Card", "Cash", "Credit Card", "Debit"]
    schema = {**_SCHEMA, "payment_method": "payment"}
    kpis = calculate_kpis(df, schema)
    assert "top_payment_methods_by_revenue" in kpis
    top = kpis["top_payment_methods_by_revenue"]
    assert len(top) >= 3
    # _top_by_revenue uses the actual column name as the key
    dim_key = [k for k in top[0] if k != "revenue"][0]
    # All three payment methods must appear
    values = {row[dim_key] for row in top}
    assert "Credit Card" in values
    assert "Cash" in values
