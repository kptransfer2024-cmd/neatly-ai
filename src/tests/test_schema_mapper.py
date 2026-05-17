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


def test_confidence_high_for_close_alternate():
    # order_date is index-1 alias (score 0.88 >= 0.85 threshold) → still "high"
    df = _df(order_date=["2024-01-01"])
    schema = infer_ecommerce_schema(df)
    assert schema["confidence"]["date"] == "high"


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


# ---------------------------------------------------------------------------
# New tests: messy/realistic column names
# ---------------------------------------------------------------------------

def test_transaction_value_maps_as_revenue():
    df = pd.DataFrame({"Transaction Value": [100.0, 200.0]})
    schema = infer_ecommerce_schema(df)
    assert schema["revenue"] == "Transaction Value"


def test_purchase_date_maps_as_date():
    df = pd.DataFrame({"Purchase Date": ["2024-01-01", "2024-01-02"]})
    schema = infer_ecommerce_schema(df)
    assert schema["date"] == "Purchase Date"


def test_transaction_date_maps_as_date():
    df = pd.DataFrame({"Transaction Date": ["2024-01-01"]})
    schema = infer_ecommerce_schema(df)
    assert schema["date"] == "Transaction Date"


def test_transaction_id_maps_as_order_id():
    df = pd.DataFrame({"Transaction ID": ["T001", "T002"]})
    schema = infer_ecommerce_schema(df)
    assert schema["order_id"] == "Transaction ID"


def test_customer_id_with_spaces():
    df = pd.DataFrame({"Customer ID": ["C001", "C002"]})
    schema = infer_ecommerce_schema(df)
    assert schema["customer_id"] == "Customer ID"


def test_return_status_maps_as_return_flag():
    df = pd.DataFrame({"Return Status": ["Returned", "Not Returned"]})
    schema = infer_ecommerce_schema(df)
    assert schema["return_flag"] == "Return Status"


def test_churn_status_maps_as_churn_flag():
    df = pd.DataFrame({"Churn Status": ["Churned", "Active"]})
    schema = infer_ecommerce_schema(df)
    assert schema["churn_flag"] == "Churn Status"


def test_payment_method_column_maps():
    df = pd.DataFrame({"Payment Method": ["Credit Card", "Cash"]})
    schema = infer_ecommerce_schema(df)
    assert schema["payment_method"] == "Payment Method"


def test_product_price_maps_as_price():
    df = pd.DataFrame({"Product Price": [9.99, 19.99]})
    schema = infer_ecommerce_schema(df)
    assert schema["price"] == "Product Price"


def test_mapping_details_present():
    df = pd.DataFrame({"Transaction Value": [100.0], "Purchase Date": ["2024-01-01"]})
    schema = infer_ecommerce_schema(df)
    assert "mapping_details" in schema
    details = schema["mapping_details"]
    assert "revenue" in details
    assert "column" in details["revenue"]
    assert "confidence" in details["revenue"]
    assert "match_type" in details["revenue"]


def test_mapping_details_match_type_exact():
    df = pd.DataFrame({"Transaction Value": [100.0]})
    schema = infer_ecommerce_schema(df)
    rev_detail = schema["mapping_details"].get("revenue", {})
    assert rev_detail.get("match_type") in ("exact_alias", "fuzzy_alias")


def test_full_messy_schema():
    """Realistic CSV columns from messy_ecommerce_sales.csv."""
    df = pd.DataFrame({
        "Transaction ID": ["T1"],
        "Customer Name": ["Alice"],
        "Customer ID": ["C1"],
        "Purchase Date": ["2024-01-15"],
        "Product Category": ["Electronics"],
        "Product Name": ["Widget"],
        "Quantity": [2],
        "Product Price": [49.99],
        "Transaction Value": [99.98],
        "Payment Method": ["Credit Card"],
        "Region": ["North"],
        "Return Status": ["Not Returned"],
        "Churn Status": ["Active"],
    })
    schema = infer_ecommerce_schema(df)
    assert schema["revenue"] == "Transaction Value"
    assert schema["date"] == "Purchase Date"
    assert schema["order_id"] == "Transaction ID"
    assert schema["customer_id"] == "Customer ID"
    assert schema["category"] == "Product Category"
    assert schema["return_flag"] == "Return Status"
    assert schema["churn_flag"] == "Churn Status"
    assert schema["payment_method"] == "Payment Method"
