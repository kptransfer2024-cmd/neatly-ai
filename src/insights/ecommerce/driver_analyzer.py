"""Revenue driver decomposition — time-based and static modes.

Time-based: requires date + revenue + ≥2 completed periods.
Static: works with any available dimension + metric, no date required.
"""
from __future__ import annotations

import pandas as pd
import numpy as np

_TRUE_VALS = frozenset({"1", "true", "yes", "returned", "refund", "y", "churned"})


# ---------------------------------------------------------------------------
# Time-based driver analysis (requires date + revenue)
# ---------------------------------------------------------------------------

def analyze_revenue_drivers(df: pd.DataFrame, schema: dict, period: str = "M") -> dict:
    """Compare latest vs previous period and decompose revenue change by all dimensions."""
    overall = _overall_change(df, schema, period)
    if "error" in overall:
        return overall

    result: dict = {"overall_change": overall}
    prev_mask, curr_mask = _period_masks(df, schema, period)

    for dim_field in ("category", "product", "region", "channel", "payment_method"):
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


# ---------------------------------------------------------------------------
# Static driver analysis (no date required)
# ---------------------------------------------------------------------------

def analyze_static_drivers(df: pd.DataFrame, schema: dict) -> dict:
    """Identify top performers and concentration by available dimensions and metrics.

    Does NOT require a date column. Works with any dimension + revenue/quantity.
    Returns per-dimension top-value tables ranked by revenue, plus flag rates.
    """
    rev_col = schema.get("revenue")
    qty_col = schema.get("quantity")
    ret_col = schema.get("return_flag")
    churn_col = schema.get("churn_flag")

    has_revenue = bool(rev_col and rev_col in df.columns)
    has_qty = bool(qty_col and qty_col in df.columns)

    if not has_revenue and not has_qty:
        return {"error": "no revenue or quantity column available for static analysis"}

    result: dict = {}
    dimensions_analyzed: list[str] = []

    for dim_field in ("category", "product", "region", "channel", "payment_method"):
        dim_col = schema.get(dim_field)
        if not dim_col or dim_col not in df.columns:
            continue
        summary = _static_dimension_table(
            df, dim_col, rev_col, qty_col, ret_col, churn_col
        )
        if summary:
            result[f"static_by_{dim_field}"] = summary
            dimensions_analyzed.append(dim_field)

    if not result:
        return {"error": "no usable dimensions found for static analysis"}

    result["dimensions_analyzed"] = dimensions_analyzed
    result["mode"] = "static"

    # Concentration: which single category drives >50% of total revenue
    if has_revenue and "static_by_category" in result:
        totals = [(r["value"], r.get("total_revenue", 0)) for r in result["static_by_category"]]
        grand_total = sum(v for _, v in totals)
        if grand_total > 0:
            top_name, top_val = max(totals, key=lambda x: x[1])
            top_pct = top_val / grand_total * 100
            result["concentration_warning"] = (
                f"'{top_name}' accounts for {top_pct:.1f}% of total revenue."
                if top_pct > 40 else None
            )

    return result


def _static_dimension_table(
    df: pd.DataFrame,
    dim_col: str,
    rev_col: str | None,
    qty_col: str | None,
    ret_col: str | None,
    churn_col: str | None,
    max_vals: int = 10,
) -> list[dict]:
    """Return per-dimension-value aggregate stats (no date required)."""
    clean = df[dim_col].astype(str).str.strip().str.title()
    unique_vals = [v for v in clean.unique() if v not in ("Nan", "None", "")]
    if len(unique_vals) > 50:
        return []  # skip high-cardinality dimensions

    rows: list[dict] = []
    for val in unique_vals:
        mask = clean == val
        row: dict = {"value": val, "count": int(mask.sum())}
        if rev_col and rev_col in df.columns:
            rev = pd.to_numeric(df.loc[mask, rev_col], errors="coerce")
            row["total_revenue"] = round(float(rev.sum()), 2)
            row["avg_revenue"] = round(float(rev.mean()), 2) if rev.notna().any() else None
        if qty_col and qty_col in df.columns:
            qty = pd.to_numeric(df.loc[mask, qty_col], errors="coerce")
            row["total_units"] = float(qty.sum())
        if ret_col and ret_col in df.columns:
            row["return_rate"] = round(float(_flag_series(df.loc[mask, ret_col]).mean()), 4)
        if churn_col and churn_col in df.columns:
            row["churn_rate"] = round(float(_flag_series(df.loc[mask, churn_col]).mean()), 4)
        rows.append(row)

    # Sort by total_revenue desc, else count desc
    if rows and "total_revenue" in rows[0]:
        rows.sort(key=lambda x: x.get("total_revenue", 0), reverse=True)
    else:
        rows.sort(key=lambda x: x["count"], reverse=True)

    return rows[:max_vals]


def _flag_series(series: pd.Series) -> pd.Series:
    if series.dtype == "bool":
        return series
    return series.astype(str).str.lower().str.strip().isin(_TRUE_VALS)


# ---------------------------------------------------------------------------
# Internal helpers for time-based analysis
# ---------------------------------------------------------------------------

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
    if len(periods) == 0:
        return []
    now_period = pd.Timestamp.now().to_period(periods.iloc[0].freqstr)
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
        result["units_effect"] = round(float((curr_qty - prev_qty) * prev_aov), 2)
        result["price_effect"] = round(float((curr_aov - prev_aov) * curr_qty), 2)

    if ret_col and ret_col in df.columns:
        returns = _flag_series(df[ret_col])
        prev_ret = float(returns[prev_mask].mean()) if prev_mask is not None else None
        curr_ret = float(returns[curr_mask].mean()) if curr_mask is not None else None
        if prev_ret is not None and curr_ret is not None:
            result["return_effect"] = round(curr_ret - prev_ret, 4)

    return result
