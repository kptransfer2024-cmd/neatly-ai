"""Tests for insights/ecommerce/schema_mapper.py."""
import pandas as pd
import pytest
from insights.ecommerce.schema_mapper import infer_ecommerce_schema, validate_schema_mapping


def _df(**kwargs):
    n = max(len(v) for v in kwargs.values())
    return pd.DataFrame({k: v if len(v) == n else v * n for k, v in kwargs.items()})


def test_exact_alias_matches():
    df = _df(date=["2024-01-01"], revenue=["100"], order_id=["O1"], customer_id=["C1"])
    schema = infer_ecommerce_schema(df)
    assert schema["date"] == "date"
    assert schema["revenue"] == "revenue"
    assert schema["order_id"] == "order_id"
    assert schema["customer_id"] == "customer_id"


def test_alternate_alias_order_date():
    df = _df(order_date=["2024-01-01"], total_sales=["100"])
    schema = infer_ecommerce_schema(df)
    assert schema["date"] == "order_date"
    assert schema["revenue"] == "total_sales"


def test_purchase_date_alias():
    df = _df(purchase_date=["2024-01-01"])
    schema = infer_ecommerce_schema(df)
    assert schema["date"] == "purchase_date"


def test_qty_alias():
    df = _df(qty=["5"])
    schema = infer_ecommerce_schema(df)
    assert schema["quantity"] == "qty"


def test_user_id_alias():
    df = _df(user_id=["U1"])
    schema = infer_ecommerce_schema(df)
    assert schema["customer_id"] == "user_id"


def test_missing_fields_return_none():
    df = _df(product=["Widget"])
    schema = infer_ecommerce_schema(df)
    assert schema["date"] is None
    assert schema["revenue"] is None


def test_missing_recommended_populated():
    df = _df(product=["Widget"])
    schema = infer_ecommerce_schema(df)
    missing = schema["missing_recommended_fields"]
    assert "date" in missing
    assert "revenue" in missing


def test_confidence_high_for_first_alias():
    df = _df(date=["2024-01-01"])
    schema = infer_ecommerce_schema(df)
    assert schema["confidence"]["date"] == "high"


def test_confidence_medium_for_alternate():
    df = _df(order_date=["2024-01-01"])
    schema = infer_ecommerce_schema(df)
    assert schema["confidence"]["date"] == "medium"


def test_confidence_low_for_missing():
    df = _df(foo=["bar"])
    schema = infer_ecommerce_schema(df)
    assert schema["confidence"]["date"] == "low"


def test_validate_schema_all_valid():
    df = _df(date=["2024-01-01"], revenue=["100"])
    schema = infer_ecommerce_schema(df)
    result = validate_schema_mapping(schema, df)
    assert result["usable"] is True
    assert result["valid"]["date"] is True


def test_validate_schema_missing_col():
    df = _df(revenue=["100"])
    schema = {"date": "nonexistent_col", "revenue": "revenue", "confidence": {}, "missing_recommended_fields": []}
    result = validate_schema_mapping(schema, df)
    assert result["usable"] is False
    assert len(result["issues"]) > 0


def test_empty_dataframe_does_not_crash():
    df = pd.DataFrame()
    schema = infer_ecommerce_schema(df)
    assert schema["date"] is None


def test_case_insensitive_match():
    df = pd.DataFrame({"ORDER_DATE": ["2024-01-01"]})
    schema = infer_ecommerce_schema(df)
    assert schema["date"] == "ORDER_DATE"
