"""Rule-based e-commerce column inference — no LLM calls, fully deterministic."""
from __future__ import annotations

import pandas as pd

_ALIASES: dict[str, list[str]] = {
    "date":        ["date", "order_date", "purchase_date", "created_at", "transaction_date"],
    "revenue":     ["revenue", "sales", "total_sales", "amount", "total_amount", "order_value", "price_total"],
    "order_id":    ["order_id", "transaction_id", "invoice_id"],
    "customer_id": ["customer_id", "user_id", "buyer_id"],
    "product":     ["product", "item", "product_name", "sku"],
    "category":    ["category", "product_category", "department"],
    "quantity":    ["quantity", "qty", "units", "units_sold"],
    "discount":    ["discount", "discount_amount", "coupon"],
    "return_flag": ["return", "returned", "refund", "is_returned"],
    "region":      ["region", "state", "country", "market"],
    "channel":     ["channel", "source", "platform"],
}

_RECOMMENDED = {"date", "revenue", "order_id", "customer_id"}


def infer_ecommerce_schema(df: pd.DataFrame) -> dict:
    """Match DataFrame columns to e-commerce semantic fields by lowercase alias lookup."""
    cols_lower = {c.lower().strip(): c for c in df.columns}
    mapping: dict[str, str | None] = {field: None for field in _ALIASES}
    confidence: dict[str, str] = {}

    for field, aliases in _ALIASES.items():
        for alias in aliases:
            if alias in cols_lower:
                mapping[field] = cols_lower[alias]
                confidence[field] = "high" if alias == aliases[0] else "medium"
                break
        if mapping[field] is None:
            confidence[field] = "low"

    missing_recommended = [f for f in _RECOMMENDED if mapping[f] is None]
    return {**mapping, "confidence": confidence, "missing_recommended_fields": missing_recommended}


def validate_schema_mapping(mapping: dict, df: pd.DataFrame) -> dict:
    """Validate that mapped columns exist in df and have usable dtypes."""
    issues: list[str] = []
    valid: dict[str, bool] = {}

    for field, col in mapping.items():
        if field in ("confidence", "missing_recommended_fields"):
            continue
        if col is None:
            valid[field] = False
            continue
        if col not in df.columns:
            issues.append(f"Mapped column '{col}' for field '{field}' not found in DataFrame.")
            valid[field] = False
            continue
        valid[field] = True

    return {"valid": valid, "issues": issues, "usable": len(issues) == 0}
