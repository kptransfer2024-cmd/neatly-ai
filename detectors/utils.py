"""Shared utilities for detector modules."""
import re
import pandas as pd

_WS_PATTERN = re.compile(r'\s+')


def severity_from_pct(pct: float, high: float = 20.0, medium: float = 5.0) -> str:
    """Return 'high', 'medium', or 'low' based on percentage thresholds.

    pct > high → 'high'
    high >= pct > medium → 'medium'
    pct <= medium → 'low'
    """
    return 'high' if pct > high else 'medium' if pct > medium else 'low'


def get_string_columns(df: pd.DataFrame) -> list[str]:
    """Return column names whose dtype is 'object' or 'str' (pandas 2.x-safe)."""
    return [c for c in df.columns if str(df[c].dtype) in ('object', 'str')]
