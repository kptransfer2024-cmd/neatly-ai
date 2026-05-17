"""Business-relevant EDA summaries — aggregate stats only, no raw row exposure."""
from __future__ import annotations

import pandas as pd

_MAX_CATEGORIES = 10


def summarize_dataset(df: pd.DataFrame, schema: dict) -> dict:
    """Return high-level dataset overview combining all EDA sub-summaries."""
    return {
        "row_count": len(df),
        "column_count": len(df.columns),
        "numeric_columns": int((df.select_dtypes("number").columns).size),
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
