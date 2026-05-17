"""Business-relevant EDA summaries — aggregate stats only, no raw row exposure."""
from __future__ import annotations

import pandas as pd

_MAX_CATEGORIES = 10
_TRUE_VALS = frozenset({"1", "true", "yes", "returned", "refund", "y", "churned"})


def summarize_dataset(df: pd.DataFrame, schema: dict) -> dict:
    """Return high-level dataset overview combining all EDA sub-summaries."""
    return {
        "row_count": len(df),
        "column_count": len(df.columns),
        "numeric_columns": int(df.select_dtypes("number").columns.size),
        "categorical_columns": int(
            df.select_dtypes(include=["object", "string", "category"]).shape[1]
        ),
        "missing_cells": int(df.isna().sum().sum()),
        "missing_pct": round(float(df.isna().mean().mean()) * 100, 2),
        "numeric_summary": summarize_numeric_columns(df),
        "categorical_summary": summarize_categorical_columns(df),
        "date_coverage": summarize_date_coverage(df, schema),
    }


def summarize_numeric_columns(df: pd.DataFrame) -> dict:
    """Return aggregate stats for each numeric column — never individual row values."""
    result: dict = {}
    for col in df.select_dtypes("number").columns:
        s = df[col].dropna()
        if len(s) == 0:
            continue
        result[col] = {
            "count": int(len(s)),
            "mean": round(float(s.mean()), 4),
            "median": round(float(s.median()), 4),
            "std": round(float(s.std()), 4),
            "min": round(float(s.min()), 4),
            "max": round(float(s.max()), 4),
            "null_pct": round(float(df[col].isna().mean()) * 100, 2),
        }
    return result


def summarize_categorical_columns(df: pd.DataFrame, max_categories: int = _MAX_CATEGORIES) -> dict:
    """Return top-value distributions for categorical columns."""
    result: dict = {}
    cat_cols = df.select_dtypes(include=["object", "string", "category"]).columns
    for col in cat_cols:
        s = df[col].dropna().astype(str)
        if len(s) == 0:
            continue
        vc = s.value_counts()
        result[col] = {
            "unique_count": int(s.nunique()),
            "null_pct": round(float(df[col].isna().mean()) * 100, 2),
            "high_cardinality": bool(s.nunique() > 50),
            "top_values": [
                {"value": str(k), "count": int(v)}
                for k, v in vc.head(max_categories).items()
            ],
        }
    return result


def summarize_date_coverage(df: pd.DataFrame, schema: dict) -> dict:
    """Return date range and period count for the date column, if mapped."""
    date_col = schema.get("date")
    if not date_col or date_col not in df.columns:
        return {}

    dates = pd.to_datetime(df[date_col], errors="coerce").dropna()
    if len(dates) == 0:
        return {"date_column": date_col, "valid_dates": 0}

    return {
        "date_column": date_col,
        "valid_dates": int(len(dates)),
        "min_date": str(dates.min().date()),
        "max_date": str(dates.max().date()),
        "date_range_days": int((dates.max() - dates.min()).days),
        "months_covered": int(dates.dt.to_period("M").nunique()),
    }


def generic_business_eda(df: pd.DataFrame, schema: dict) -> dict:
    """Compute flexible EDA that works regardless of schema completeness.

    Analyzes available dimensions and metrics without requiring date or revenue.
    Returns category-level summaries, binary rates, and dimension distributions.
    """
    result: dict = {}

    rev_col = schema.get("revenue")
    cat_col = schema.get("category")
    prod_col = schema.get("product")
    reg_col = schema.get("region")
    chan_col = schema.get("channel")
    pay_col = schema.get("payment_method")
    qty_col = schema.get("quantity")
    ret_col = schema.get("return_flag")
    churn_col = schema.get("churn_flag")

    # -----------------------------------------------------------------------
    # Category-level summaries
    # -----------------------------------------------------------------------
    for dim_field, dim_col in [
        ("category", cat_col), ("product", prod_col),
        ("region", reg_col), ("channel", chan_col), ("payment_method", pay_col),
    ]:
        if dim_col and dim_col in df.columns:
            dim_summary = _dimension_summary(df, dim_col, rev_col, qty_col, ret_col, churn_col)
            result[f"{dim_field}_summary"] = dim_summary

    # -----------------------------------------------------------------------
    # Binary rates (overall)
    # -----------------------------------------------------------------------
    if ret_col and ret_col in df.columns:
        flag = _flag_series(df[ret_col])
        result["overall_return_rate"] = round(float(flag.mean()), 4)

    if churn_col and churn_col in df.columns:
        flag = _flag_series(df[churn_col])
        result["overall_churn_rate"] = round(float(flag.mean()), 4)

    # -----------------------------------------------------------------------
    # Numeric correlations (only if ≥2 numeric cols and ≤10 total)
    # -----------------------------------------------------------------------
    num_cols = df.select_dtypes("number").columns.tolist()
    if 2 <= len(num_cols) <= 10:
        try:
            corr = df[num_cols].corr(numeric_only=True)
            pairs: list[dict] = []
            for i in range(len(num_cols)):
                for j in range(i + 1, len(num_cols)):
                    val = corr.iloc[i, j]
                    if abs(val) >= 0.5:
                        pairs.append({
                            "col_a": num_cols[i],
                            "col_b": num_cols[j],
                            "correlation": round(float(val), 3),
                        })
            if pairs:
                result["numeric_correlations"] = sorted(pairs, key=lambda x: -abs(x["correlation"]))[:5]
        except Exception:
            pass

    return result


def _flag_series(series: pd.Series) -> pd.Series:
    """Normalize a boolean/categorical flag to a boolean Series."""
    if series.dtype == "bool":
        return series
    return series.astype(str).str.lower().str.strip().isin(_TRUE_VALS)


def _dimension_summary(
    df: pd.DataFrame,
    dim_col: str,
    rev_col: str | None,
    qty_col: str | None,
    ret_col: str | None,
    churn_col: str | None,
    max_vals: int = 15,
) -> list[dict]:
    """Return per-dimension-value aggregate stats."""
    clean_dim = df[dim_col].astype(str).str.strip().str.title()
    groups = clean_dim.unique()
    if len(groups) > 50:
        return []  # skip high-cardinality dimensions

    rows: list[dict] = []
    for grp in groups:
        if grp in ("Nan", "None", ""):
            continue
        mask = clean_dim == grp
        row: dict = {"value": grp, "count": int(mask.sum())}
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

    # Sort by revenue desc if available, else count desc
    if rows and "total_revenue" in rows[0]:
        rows.sort(key=lambda x: x.get("total_revenue", 0), reverse=True)
    else:
        rows.sort(key=lambda x: x["count"], reverse=True)

    return rows[:max_vals]
