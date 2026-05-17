"""Number and label formatting utilities for the insights UI."""
from __future__ import annotations


def fmt_currency(value: float | None, decimals: int = 2) -> str:
    """Format a float as a currency string (e.g. $1,234.56)."""
    if value is None:
        return "N/A"
    return f"${value:,.{decimals}f}"


def fmt_pct(value: float | None, decimals: int = 1) -> str:
    """Format a ratio (0.0–1.0) as a percentage string (e.g. 12.3%)."""
    if value is None:
        return "N/A"
    return f"{value * 100:.{decimals}f}%"


def fmt_number(value: float | int | None, decimals: int = 0) -> str:
    """Format a number with thousands separator (e.g. 1,234,567)."""
    if value is None:
        return "N/A"
    if decimals == 0:
        return f"{int(value):,}"
    return f"{value:,.{decimals}f}"


def fmt_delta(value: float | None, prefix: str = "") -> str:
    """Format a signed delta value with + or - sign (e.g. +5.2% or -$1,234)."""
    if value is None:
        return "N/A"
    sign = "+" if value >= 0 else ""
    return f"{sign}{prefix}{value:,.2f}"


def severity_emoji(severity: str) -> str:
    """Return a visual indicator for anomaly severity."""
    return {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(severity, "⚪")


def trend_emoji(label: str) -> str:
    """Return a visual indicator for trend direction."""
    return {
        "improving": "📈",
        "declining": "📉",
        "flat": "➡️",
        "volatile": "〰️",
    }.get(label, "❓")


def shorten_label(label: str, max_len: int = 25) -> str:
    """Truncate a label string for display in tight UI spaces."""
    if len(label) <= max_len:
        return label
    return label[: max_len - 1] + "…"
