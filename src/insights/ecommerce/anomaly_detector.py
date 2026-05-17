"""Revenue and KPI anomaly detection using z-score, IQR, and pct-change methods."""
from __future__ import annotations

import pandas as pd
import numpy as np

_ZSCORE_HIGH = 3.0
_ZSCORE_MED = 2.0
_IQR_MULTIPLIER = 1.5
_PCT_CHANGE_HIGH = 30.0
_PCT_CHANGE_MED = 15.0
_RETURN_SPIKE_THRESHOLD = 0.3


def detect_revenue_anomalies(df: pd.DataFrame, schema: dict, period: str = "D") -> list[dict]:
    """Detect anomalous revenue periods using z-score, IQR, and large pct-change methods."""
    date_col = schema.get("date")
    rev_col = schema.get("revenue")
    if not date_col or not rev_col:
        return []
    if date_col not in df.columns or rev_col not in df.columns:
        return []

    dates = pd.to_datetime(df[date_col], errors="coerce")
    rev = pd.to_numeric(df[rev_col], errors="coerce")
    work = pd.DataFrame({"_date": dates, "_rev": rev}).dropna(subset=["_date"])
    work["_period"] = work["_date"].dt.to_period(period).astype(str)

    grouped = work.groupby("_period")["_rev"].sum().sort_index()
    if len(grouped) < 4:
        return []

    anomalies: list[dict] = []
    anomalies.extend(_zscore_anomalies(grouped, "revenue"))
    anomalies.extend(_iqr_anomalies(grouped, "revenue"))
    anomalies.extend(_pct_change_anomalies(grouped, "revenue"))
    return _deduplicate(anomalies)


def detect_kpi_anomalies(df: pd.DataFrame, schema: dict) -> dict:
    """Detect anomalies across multiple KPI dimensions."""
    result: dict = {}

    rev_anomalies = detect_revenue_anomalies(df, schema, period="M")
    if rev_anomalies:
        result["revenue"] = rev_anomalies

    ret_col = schema.get("return_flag")
    date_col = schema.get("date")
    if ret_col and ret_col in df.columns and date_col and date_col in df.columns:
        ret_anomalies = _detect_return_rate_spike(df, schema)
        if ret_anomalies:
            result["return_rate"] = ret_anomalies

    ord_col = schema.get("order_id")
    if ord_col and ord_col in df.columns and date_col and date_col in df.columns:
        ord_anomalies = _detect_order_volume_drop(df, schema)
        if ord_anomalies:
            result["order_volume"] = ord_anomalies

    return result


def _zscore_anomalies(series: pd.Series, metric: str) -> list[dict]:
    """Flag periods where value is more than 2 or 3 standard deviations from mean."""
    mean = float(series.mean())
    std = float(series.std())
    if std == 0:
        return []
    results: list[dict] = []
    for period_str, value in series.items():
        z = (float(value) - mean) / std
        if abs(z) >= _ZSCORE_HIGH:
            severity = "high"
        elif abs(z) >= _ZSCORE_MED:
            severity = "medium"
        else:
            continue
        results.append(_make_anomaly(metric, str(period_str), float(value), "z_score", severity, z))
    return results


def _iqr_anomalies(series: pd.Series, metric: str) -> list[dict]:
    """Flag periods beyond Tukey IQR fence."""
    q1 = float(series.quantile(0.25))
    q3 = float(series.quantile(0.75))
    iqr = q3 - q1
    if iqr == 0:
        return []
    lower = q1 - _IQR_MULTIPLIER * iqr
    upper = q3 + _IQR_MULTIPLIER * iqr
    results: list[dict] = []
    for period_str, value in series.items():
        v = float(value)
        if v < lower or v > upper:
            severity = "high" if (v < lower - iqr or v > upper + iqr) else "medium"
            results.append(_make_anomaly(metric, str(period_str), v, "iqr", severity, None))
    return results


def _pct_change_anomalies(series: pd.Series, metric: str) -> list[dict]:
    """Flag periods with unusually large period-over-period % change."""
    pct = series.pct_change() * 100
    results: list[dict] = []
    for period_str, value in pct.items():
        if pd.isna(value):
            continue
        abs_val = abs(float(value))
        if abs_val >= _PCT_CHANGE_HIGH:
            severity = "high"
        elif abs_val >= _PCT_CHANGE_MED:
            severity = "medium"
        else:
            continue
        results.append(_make_anomaly(
            metric, str(period_str), float(series[period_str]), "pct_change", severity, float(value)
        ))
    return results


def _detect_return_rate_spike(df: pd.DataFrame, schema: dict) -> list[dict]:
    """Detect monthly return rate spikes above threshold."""
    date_col = schema.get("date")
    ret_col = schema.get("return_flag")
    dates = pd.to_datetime(df[date_col], errors="coerce")
    true_vals = {"1", "true", "yes", "returned", "refund", "y"}
    returns = df[ret_col].astype(str).str.lower().str.strip().isin(true_vals)
    work = pd.DataFrame({"_date": dates, "_ret": returns}).dropna(subset=["_date"])
    work["_period"] = work["_date"].dt.to_period("M").astype(str)
    rate = work.groupby("_period")["_ret"].mean()
    results: list[dict] = []
    for period_str, value in rate.items():
        if float(value) > _RETURN_SPIKE_THRESHOLD:
            results.append(_make_anomaly("return_rate", str(period_str), float(value), "pct_change", "high", None))
    return results


def _detect_order_volume_drop(df: pd.DataFrame, schema: dict) -> list[dict]:
    """Detect periods with unusually low order volume."""
    date_col = schema.get("date")
    ord_col = schema.get("order_id")
    dates = pd.to_datetime(df[date_col], errors="coerce")
    work = df.assign(_date=dates).dropna(subset=["_date"])
    work["_period"] = work["_date"].dt.to_period("M").astype(str)
    counts = work.groupby("_period")[ord_col].count()
    return _zscore_anomalies(counts, "order_volume")


def _make_anomaly(
    metric: str, period: str, value: float, method: str, severity: str, z_or_pct: float | None
) -> dict:
    """Construct a standardized anomaly dict."""
    direction = "below" if z_or_pct is not None and z_or_pct < 0 else "above"
    summary = (
        f"{metric.replace('_', ' ').title()} in {period} appears anomalously {direction} normal range "
        f"(detected via {method.replace('_', '-')} method). This may indicate a data issue or a real business event."
    )
    return {
        "metric": metric,
        "period": period,
        "value": round(value, 4),
        "method": method,
        "severity": severity,
        "explanation_ready_summary": summary,
    }


def _deduplicate(anomalies: list[dict]) -> list[dict]:
    """Keep highest-severity anomaly per (metric, period) pair."""
    _SEV_ORDER = {"high": 2, "medium": 1, "low": 0}
    seen: dict[tuple, dict] = {}
    for a in anomalies:
        key = (a["metric"], a["period"])
        if key not in seen or _SEV_ORDER[a["severity"]] > _SEV_ORDER[seen[key]["severity"]]:
            seen[key] = a
    return list(seen.values())
