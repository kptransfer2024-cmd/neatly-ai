"""Revenue driver decomposition by dimension (category/product/region/channel)."""
from __future__ import annotations

import pandas as pd
import numpy as np


def analyze_revenue_drivers(df: pd.DataFrame, schema: dict, period: str = "M") -> dict:
    """Compare latest vs previous period and decompose revenue change by all dimensions."""
    overall = _overall_change(df, schema, period)
    if "error" in overall:
        return overall

    result: dict = {"overall_change": overall}
    prev_mask, curr_mask = _period_masks(df, schema, period)

    for dim_field in ("category", "product", "region", "channel"):
        dim_col = schema.get(dim_field)
        if dim_col and dim_col in df.columns:
            drivers = decompose_revenue_change_by_dimension(df, schema, dim_field, period)
            result[f"drivers_by_{dim_field}"] = drivers.get("drivers", [])

    result["mechanism_summary"] = _mechanism_summary(df, schema, prev_mask, curr_mask)
    return result


def analyze_latest_period_drivers(df: pd.DataFrame, schema: dict, period: str = "M") -> dict:
    """Wrapper returning only the overall change and driver tables."""
    return analyze_revenue_drivers(df, schema, period)


def decompose_revenue_change_by_dimension(
    df: pd.DataFrame, schema: dict, dimension: str, period: str = "M"
) -> dict:
    """Break down revenue change contribution by a single dimension."""
    date_col = schema.get("date")
    rev_col = schema.get("revenue")
    dim_col = schema.get(dimension)

    if not all([date_col, rev_col, dim_col]):
        return {"error": f"missing mapping for date, revenue, or {dimension}"}
    for c in [date_col, rev_col, dim_col]:
        if c not in df.columns:
            return {"error": f"column '{c}' not found in DataFrame"}

    dates = pd.to_datetime(df[date_col], errors="coerce")
    rev = pd.to_numeric(df[rev_col], errors="coerce")
    work = pd.DataFrame({"_date": dates, "_rev": rev, "_dim": df[dim_col]}).dropna(subset=["_date"])
    work["_period"] = work["_date"].dt.to_period(period)

    completed = _completed_periods(work["_period"])
    if len(completed) < 2:
        return {"error": "need at least 2 completed periods for comparison"}

    prev_p, curr_p = completed[-2], completed[-1]
    prev_df = work[work["_period"] == prev_p]
    curr_df = work[work["_period"] == curr_p]

    prev_by_dim = prev_df.groupby("_dim")["_rev"].sum()
    curr_by_dim = curr_df.groupby("_dim")["_rev"].sum()
    all_dims = prev_by_dim.index.union(curr_by_dim.index)

    total_change = float(curr_df["_rev"].sum() - prev_df["_rev"].sum())

    drivers: list[dict] = []
    for dim_val in all_dims:
        prev_rev = float(prev_by_dim.get(dim_val, 0.0))
        curr_rev = float(curr_by_dim.get(dim_val, 0.0))
        change = curr_rev - prev_rev
        contribution_pct = round(change / abs(total_change) * 100, 2) if total_change != 0 else 0.0
        drivers.append({
            "dimension_value": str(dim_val),
            "previous_revenue": round(prev_rev, 2),
            "latest_revenue": round(curr_rev, 2),
            "absolute_change": round(change, 2),
            "contribution_to_total_change_pct": contribution_pct,
        })

    drivers.sort(key=lambda x: x["contribution_to_total_change_pct"])
    return {
        "dimension": dimension,
        "dimension_column": dim_col,
        "previous_period": str(prev_p),
        "latest_period": str(curr_p),
        "total_revenue_change": round(total_change, 2),
        "drivers": drivers,
    }


def _overall_change(df: pd.DataFrame, schema: dict, period: str) -> dict:
    """Compute aggregate revenue change between last two completed periods."""
    date_col = schema.get("date")
    rev_col = schema.get("revenue")
    if not date_col or not rev_col:
        return {"error": "date or revenue not mapped"}
    if date_col not in df.columns or rev_col not in df.columns:
        return {"error": "mapped columns not found"}

    dates = pd.to_datetime(df[date_col], errors="coerce")
    rev = pd.to_numeric(df[rev_col], errors="coerce")
    work = pd.DataFrame({"_date": dates, "_rev": rev}).dropna(subset=["_date"])
    work["_period"] = work["_date"].dt.to_period(period)

    completed = _completed_periods(work["_period"])
    if len(completed) < 2:
        return {"error": "need at least 2 completed periods"}

    prev_p, curr_p = completed[-2], completed[-1]
    prev_rev = float(work[work["_period"] == prev_p]["_rev"].sum())
    curr_rev = float(work[work["_period"] == curr_p]["_rev"].sum())
    change = curr_rev - prev_rev
    change_pct = round(change / abs(prev_rev) * 100, 2) if prev_rev != 0 else None

    return {
        "previous_period": str(prev_p),
        "latest_period": str(curr_p),
        "previous_revenue": round(prev_rev, 2),
        "latest_revenue": round(curr_rev, 2),
        "change": round(change, 2),
        "change_pct": change_pct,
    }


def _period_masks(df: pd.DataFrame, schema: dict, period: str):
    """Return boolean masks for previous and current completed periods."""
    date_col = schema.get("date")
    if not date_col or date_col not in df.columns:
        return None, None
    dates = pd.to_datetime(df[date_col], errors="coerce")
    periods = dates.dt.to_period(period)
    completed = _completed_periods(periods.dropna())
    if len(completed) < 2:
        return None, None
    return periods == completed[-2], periods == completed[-1]


def _completed_periods(periods: pd.Series) -> list:
    """Return sorted list of unique completed (non-current) periods."""
    now_period = pd.Timestamp.now().to_period(periods.iloc[0].freqstr if len(periods) > 0 else "M")
    unique = sorted(periods.unique())
    return [p for p in unique if p < now_period]


def _mechanism_summary(
    df: pd.DataFrame, schema: dict, prev_mask, curr_mask
) -> dict:
    """Decompose change into units effect vs price/AOV effect if quantity is available."""
    rev_col = schema.get("revenue")
    qty_col = schema.get("quantity")
    ret_col = schema.get("return_flag")

    if prev_mask is None or curr_mask is None:
        return {}

    result: dict = {}

    if rev_col and qty_col and rev_col in df.columns and qty_col in df.columns:
        prev_rev = pd.to_numeric(df.loc[prev_mask, rev_col], errors="coerce").sum()
        curr_rev = pd.to_numeric(df.loc[curr_mask, rev_col], errors="coerce").sum()
        prev_qty = pd.to_numeric(df.loc[prev_mask, qty_col], errors="coerce").sum()
        curr_qty = pd.to_numeric(df.loc[curr_mask, qty_col], errors="coerce").sum()
        prev_aov = prev_rev / prev_qty if prev_qty > 0 else 0.0
        curr_aov = curr_rev / curr_qty if curr_qty > 0 else 0.0
        units_effect = round(float((curr_qty - prev_qty) * prev_aov), 2)
        price_effect = round(float((curr_aov - prev_aov) * curr_qty), 2)
        result["units_effect"] = units_effect
        result["price_effect"] = price_effect

    if ret_col and ret_col in df.columns:
        true_vals = {"1", "true", "yes", "returned", "refund", "y"}
        returns = df[ret_col].astype(str).str.lower().str.strip().isin(true_vals)
        prev_ret = returns[prev_mask].mean() if prev_mask is not None else None
        curr_ret = returns[curr_mask].mean() if curr_mask is not None else None
        if prev_ret is not None and curr_ret is not None:
            result["return_effect"] = round(float(curr_ret - prev_ret), 4)

    return result
