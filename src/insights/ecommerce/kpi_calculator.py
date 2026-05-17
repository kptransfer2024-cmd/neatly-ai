"""Deterministic KPI metrics for e-commerce sales data — no LLM, no raw rows exposed."""
from __future__ import annotations

import pandas as pd


def _col(schema: dict, field: str) -> str | None:
    """Return mapped column name for field, or None if not mapped."""
    return schema.get(field)


def calculate_kpis(df: pd.DataFrame, schema: dict) -> dict:
    """Calculate aggregate e-commerce KPIs from a cleaned DataFrame."""
    result: dict = {}
    rev_col = _col(schema, "revenue")
    ord_col = _col(schema, "order_id")
    cust_col = _col(schema, "customer_id")
    qty_col = _col(schema, "quantity")
    disc_col = _col(schema, "discount")
    ret_col = _col(schema, "return_flag")
    cat_col = _col(schema, "category")
    prod_col = _col(schema, "product")
    reg_col = _col(schema, "region")
    chan_col = _col(schema, "channel")

    if rev_col and rev_col in df.columns:
        rev = pd.to_numeric(df[rev_col], errors="coerce")
        result["total_revenue"] = float(rev.sum())
        result["average_order_value"] = float(rev.mean()) if len(rev.dropna()) else None

    if ord_col and ord_col in df.columns:
        result["total_orders"] = int(df[ord_col].nunique())

    if cust_col and cust_col in df.columns:
        result["unique_customers"] = int(df[cust_col].nunique())
        if rev_col and rev_col in df.columns:
            rev = pd.to_numeric(df[rev_col], errors="coerce")
            result["revenue_per_customer"] = float(
                rev.sum() / max(df[cust_col].nunique(), 1)
            )

    if qty_col and qty_col in df.columns:
        qty = pd.to_numeric(df[qty_col], errors="coerce")
        result["total_units_sold"] = float(qty.sum())
        if rev_col and rev_col in df.columns:
            rev = pd.to_numeric(df[rev_col], errors="coerce")
            total_qty = qty.sum()
            result["average_unit_price"] = float(rev.sum() / total_qty) if total_qty > 0 else None

    if ret_col and ret_col in df.columns:
        returns = df[ret_col]
        if str(returns.dtype) in ("bool",):
            return_rate = float(returns.mean())
        else:
            true_vals = {"1", "true", "yes", "returned", "refund", "y"}
            return_rate = float(returns.astype(str).str.lower().str.strip().isin(true_vals).mean())
        result["return_rate"] = return_rate

    if disc_col and disc_col in df.columns:
        disc = pd.to_numeric(df[disc_col], errors="coerce")
        result["total_discount"] = float(disc.sum())
        if rev_col and rev_col in df.columns:
            rev = pd.to_numeric(df[rev_col], errors="coerce")
            total_with_disc = rev.sum() + disc.sum()
            result["discount_rate"] = float(disc.sum() / total_with_disc) if total_with_disc > 0 else 0.0

    if rev_col and rev_col in df.columns:
        rev = pd.to_numeric(df[rev_col], errors="coerce")
        if prod_col and prod_col in df.columns:
            result["top_products_by_revenue"] = _top_by_revenue(df, prod_col, rev_col, 10)
        if cat_col and cat_col in df.columns:
            result["top_categories_by_revenue"] = _top_by_revenue(df, cat_col, rev_col, 10)
        if reg_col and reg_col in df.columns:
            result["top_regions_by_revenue"] = _top_by_revenue(df, reg_col, rev_col, 10)
        if chan_col and chan_col in df.columns:
            result["top_channels_by_revenue"] = _top_by_revenue(df, chan_col, rev_col, 10)

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
