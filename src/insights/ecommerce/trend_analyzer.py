"""Period-over-period revenue trend analysis — deterministic pandas, no LLM."""
from __future__ import annotations

import pandas as pd
import numpy as np

_VOLATILITY_CV_THRESHOLD = 0.25
_FLAT_PCT_THRESHOLD = 5.0


def analyze_revenue_trend(df: pd.DataFrame, schema: dict, period: str = "M") -> dict:
    """Compute period revenue trend with labels, rolling average, and highlights."""
    date_col = schema.get("date")
    rev_col = schema.get("revenue")
    if not date_col or not rev_col:
        return {"error": "date or revenue column not mapped"}
    if date_col not in df.columns or rev_col not in df.columns:
        return {"error": "mapped columns not found in DataFrame"}

    dates = pd.to_datetime(df[date_col], errors="coerce")
    rev = pd.to_numeric(df[rev_col], errors="coerce")
    work = pd.DataFrame({"_date": dates, "_rev": rev}).dropna(subset=["_date"])
    work["_period"] = work["_date"].dt.to_period(period)

    grouped = work.groupby("_period")["_rev"].sum().sort_index()
    if len(grouped) == 0:
        return {"error": "no valid date/revenue data after parsing"}

    periods = grouped.index.astype(str).tolist()
    revenues = grouped.values.tolist()

    pct_changes = _pct_change_list(revenues)
    rolling_avg = _rolling_avg(revenues, window=4) if len(revenues) >= 4 else []
    trend_label = _classify_trend(revenues, pct_changes)
    best_period = periods[int(np.argmax(revenues))]
    worst_period = periods[int(np.argmin(revenues))]
    largest_drop_idx = _largest_change_idx(pct_changes, direction="drop")
    largest_increase_idx = _largest_change_idx(pct_changes, direction="increase")
    notes = _build_notes(revenues, pct_changes, periods, trend_label)

    return {
        "periods": periods,
        "revenues": revenues,
        "pct_changes": pct_changes,
        "rolling_avg": rolling_avg,
        "trend_label": trend_label,
        "best_period": best_period,
        "worst_period": worst_period,
        "latest_period": periods[-1],
        "latest_revenue": revenues[-1],
        "largest_drop": {"period": periods[largest_drop_idx], "pct_change": pct_changes[largest_drop_idx]} if largest_drop_idx is not None else None,
        "largest_increase": {"period": periods[largest_increase_idx], "pct_change": pct_changes[largest_increase_idx]} if largest_increase_idx is not None else None,
        "notes": notes,
    }


def analyze_kpi_trends(df: pd.DataFrame, schema: dict, period: str = "M") -> dict:
    """Compute trends for revenue, order count, and units by period."""
    result: dict = {}

    rev_trend = analyze_revenue_trend(df, schema, period)
    if "error" not in rev_trend:
        result["revenue_trend"] = rev_trend

    date_col = schema.get("date")
    ord_col = schema.get("order_id")
    qty_col = schema.get("quantity")

    if date_col and date_col in df.columns and ord_col and ord_col in df.columns:
        dates = pd.to_datetime(df[date_col], errors="coerce")
        work = df.assign(_date=dates).dropna(subset=["_date"])
        work["_period"] = work["_date"].dt.to_period(period).astype(str)
        order_counts = work.groupby("_period")[ord_col].nunique().sort_index()
        result["order_count_trend"] = {
            "periods": order_counts.index.tolist(),
            "values": order_counts.values.tolist(),
            "pct_changes": _pct_change_list(order_counts.values.tolist()),
        }

    if date_col and date_col in df.columns and qty_col and qty_col in df.columns:
        dates = pd.to_datetime(df[date_col], errors="coerce")
        qty = pd.to_numeric(df[qty_col], errors="coerce")
        work = pd.DataFrame({"_date": dates, "_qty": qty}).dropna(subset=["_date"])
        work["_period"] = work["_date"].dt.to_period(period).astype(str)
        unit_totals = work.groupby("_period")["_qty"].sum().sort_index()
        result["units_trend"] = {
            "periods": unit_totals.index.tolist(),
            "values": unit_totals.values.tolist(),
            "pct_changes": _pct_change_list(unit_totals.values.tolist()),
        }

    return result


def _pct_change_list(values: list[float]) -> list[float | None]:
    """Compute period-over-period % changes; first element is None."""
    result: list[float | None] = [None]
    for i in range(1, len(values)):
        prev = values[i - 1]
        if prev == 0:
            result.append(None)
        else:
            result.append(round((values[i] - prev) / abs(prev) * 100, 2))
    return result


def _rolling_avg(values: list[float], window: int = 4) -> list[float | None]:
    """Compute rolling average; returns None for positions before window is full."""
    result: list[float | None] = [None] * (window - 1)
    for i in range(window - 1, len(values)):
        result.append(round(float(np.mean(values[i - window + 1: i + 1])), 4))
    return result


def _classify_trend(revenues: list[float], pct_changes: list[float | None]) -> str:
    """Assign trend_label based on direction consistency and volatility."""
    valid_changes = [c for c in pct_changes if c is not None]
    if not valid_changes:
        return "flat"

    avg_change = float(np.mean(valid_changes))
    positive = sum(1 for c in valid_changes if c > _FLAT_PCT_THRESHOLD)
    negative = sum(1 for c in valid_changes if c < -_FLAT_PCT_THRESHOLD)
    n = len(valid_changes)

    # Volatile: large swings in BOTH directions
    if positive >= 2 and negative >= 2:
        return "volatile"

    if avg_change > _FLAT_PCT_THRESHOLD and positive > negative:
        return "improving"
    if avg_change < -_FLAT_PCT_THRESHOLD and negative > positive:
        return "declining"
    return "flat"


def _largest_change_idx(pct_changes: list[float | None], direction: str) -> int | None:
    """Return index of the largest drop or increase, or None if not found."""
    valid = [(i, c) for i, c in enumerate(pct_changes) if c is not None]
    if not valid:
        return None
    if direction == "drop":
        return min(valid, key=lambda x: x[1])[0]
    return max(valid, key=lambda x: x[1])[0]


def _build_notes(
    revenues: list[float],
    pct_changes: list[float | None],
    periods: list[str],
    trend_label: str,
) -> list[str]:
    """Generate human-readable notes about the trend."""
    notes: list[str] = []
    notes.append(f"Overall trend appears {trend_label} across {len(periods)} periods.")
    valid = [(p, c) for p, c in zip(periods[1:], pct_changes[1:]) if c is not None]
    if valid:
        worst_p, worst_c = min(valid, key=lambda x: x[1])
        notes.append(f"Largest single-period decline was {worst_c:.1f}% in {worst_p}.")
        best_p, best_c = max(valid, key=lambda x: x[1])
        if best_c > 0:
            notes.append(f"Strongest single-period growth was +{best_c:.1f}% in {best_p}.")
    return notes
