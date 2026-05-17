"""Deterministic KPI metrics for e-commerce sales data — no LLM, no raw rows exposed."""
from __future__ import annotations

import pandas as pd

_TRUE_VALS = frozenset({"1", "true", "yes", "returned", "refund", "y", "churned"})


def _col(schema: dict, field: str) -> str | None:
    """Return mapped column name for field, or None if not mapped."""
    val = schema.get(field)
    return val if isinstance(val, str) else None


def _revenue_label(schema: dict) -> str:
    """Return a human-readable label for the revenue/transaction value column."""
    rev_col = _col(schema, "revenue")
    if rev_col is None:
        return "Revenue"
    details = schema.get("mapping_details", {}).get("revenue", {})
    match_type = details.get("match_type", "")
    # If the mapped column name strongly suggests it's transaction value, label accordingly
    col_lower = rev_col.lower()
    if "transaction" in col_lower and "value" in col_lower:
        return "Total Transaction Value"
    if "purchase" in col_lower and "amount" in col_lower:
        return "Total Purchase Amount"
    if any(w in col_lower for w in ("total", "sales", "gross", "net")):
        return "Total Revenue"
    if match_type == "dtype_inference":
        return f"Total {rev_col} (proxy)"
    return "Total Revenue"


def _flag_series(series: pd.Series) -> pd.Series:
    """Normalize a return/churn flag column to boolean Series."""
    if series.dtype == "bool":
        return series
    return series.astype(str).str.lower().str.strip().isin(_TRUE_VALS)


def calculate_kpis(df: pd.DataFrame, schema: dict) -> dict:
    """Calculate aggregate e-commerce KPIs from a cleaned DataFrame."""
    result: dict = {}

    rev_col = _col(schema, "revenue")
    ord_col = _col(schema, "order_id")
    cust_col = _col(schema, "customer_id")
    qty_col = _col(schema, "quantity")
    disc_col = _col(schema, "discount")
    ret_col = _col(schema, "return_flag")
    churn_col = _col(schema, "churn_flag")
    cat_col = _col(schema, "category")
    prod_col = _col(schema, "product")
    reg_col = _col(schema, "region")
    chan_col = _col(schema, "channel")
    pay_col = _col(schema, "payment_method")
    price_col = _col(schema, "price")

    # Revenue metrics
    if rev_col and rev_col in df.columns:
        rev = pd.to_numeric(df[rev_col], errors="coerce")
        result["total_revenue"] = float(rev.sum())
        result["average_order_value"] = float(rev.mean()) if len(rev.dropna()) > 0 else None
        result["revenue_label"] = _revenue_label(schema)
        result["revenue_source_column"] = rev_col

    # Order metrics
    if ord_col and ord_col in df.columns:
        result["total_orders"] = int(df[ord_col].nunique())

    # Customer metrics
    if cust_col and cust_col in df.columns:
        result["unique_customers"] = int(df[cust_col].nunique())
        if rev_col and rev_col in df.columns:
            rev = pd.to_numeric(df[rev_col], errors="coerce")
            result["revenue_per_customer"] = float(
                rev.sum() / max(df[cust_col].nunique(), 1)
            )

    # Quantity / price metrics
    if qty_col and qty_col in df.columns:
        qty = pd.to_numeric(df[qty_col], errors="coerce")
        result["total_units_sold"] = float(qty.sum())
        if rev_col and rev_col in df.columns:
            rev = pd.to_numeric(df[rev_col], errors="coerce")
            total_qty = qty.sum()
            result["average_unit_price"] = float(rev.sum() / total_qty) if total_qty > 0 else None
    elif price_col and price_col in df.columns:
        price = pd.to_numeric(df[price_col], errors="coerce")
        result["average_unit_price"] = float(price.mean()) if len(price.dropna()) > 0 else None

    # Return rate
    if ret_col and ret_col in df.columns:
        result["return_rate"] = float(_flag_series(df[ret_col]).mean())

    # Churn rate
    if churn_col and churn_col in df.columns:
        result["churn_rate"] = float(_flag_series(df[churn_col]).mean())

    # Discount metrics
    if disc_col and disc_col in df.columns:
        disc = pd.to_numeric(df[disc_col], errors="coerce")
        result["total_discount"] = float(disc.sum())
        if rev_col and rev_col in df.columns:
            rev = pd.to_numeric(df[rev_col], errors="coerce")
            total_with_disc = rev.sum() + disc.sum()
            result["discount_rate"] = float(disc.sum() / total_with_disc) if total_with_disc > 0 else 0.0

    # Top performers by revenue
    if rev_col and rev_col in df.columns:
        if prod_col and prod_col in df.columns:
            result["top_products_by_revenue"] = _top_by_revenue(df, prod_col, rev_col, 10)
        if cat_col and cat_col in df.columns:
            result["top_categories_by_revenue"] = _top_by_revenue(df, cat_col, rev_col, 10)
        if reg_col and reg_col in df.columns:
            result["top_regions_by_revenue"] = _top_by_revenue(df, reg_col, rev_col, 10)
        if chan_col and chan_col in df.columns:
            result["top_channels_by_revenue"] = _top_by_revenue(df, chan_col, rev_col, 10)
        if pay_col and pay_col in df.columns:
            result["top_payment_methods_by_revenue"] = _top_by_revenue(df, pay_col, rev_col, 10)

    return result


def _top_by_revenue(df: pd.DataFrame, group_col: str, rev_col: str, n: int) -> list[dict]:
    """Return top-n groups by summed revenue as list of dicts."""
    rev = pd.to_numeric(df[rev_col], errors="coerce")
    grouped = (
        df.assign(_rev=rev)
        .groupby(group_col, dropna=False)["_rev"]
        .sum()
        .nlargest(n)
        .reset_index()
    )
    grouped.columns = [group_col, "revenue"]
    return grouped.to_dict("records")


def calculate_period_kpis(df: pd.DataFrame, schema: dict, period: str = "M") -> dict:
    """Calculate revenue, order count, and units by time period."""
    date_col = _col(schema, "date")
    rev_col = _col(schema, "revenue")
    ord_col = _col(schema, "order_id")
    qty_col = _col(schema, "quantity")

    if not date_col or date_col not in df.columns:
        return {}
    if not rev_col or rev_col not in df.columns:
        return {}

    dates = pd.to_datetime(df[date_col], errors="coerce")
    rev = pd.to_numeric(df[rev_col], errors="coerce")
    work = df.assign(_date=dates, _rev=rev).dropna(subset=["_date"])
    work["_period"] = work["_date"].dt.to_period(period).astype(str)

    result: dict = {}
    result["revenue_by_period"] = (
        work.groupby("_period")["_rev"].sum().reset_index()
        .rename(columns={"_period": "period", "_rev": "revenue"})
        .to_dict("records")
    )

    if ord_col and ord_col in df.columns:
        result["order_count_by_period"] = (
            work.groupby("_period")[ord_col].nunique().reset_index()
            .rename(columns={"_period": "period", ord_col: "order_count"})
            .to_dict("records")
        )

    if qty_col and qty_col in df.columns:
        qty = pd.to_numeric(df[qty_col], errors="coerce")
        work2 = work.assign(_qty=qty.reindex(work.index))
        result["units_by_period"] = (
            work2.groupby("_period")["_qty"].sum().reset_index()
            .rename(columns={"_period": "period", "_qty": "units"})
            .to_dict("records")
        )

    return result
