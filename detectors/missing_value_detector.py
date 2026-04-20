"""Detects missing / null values in a DataFrame and returns structured issue dicts."""
import pandas as pd


def detect_missing(df: pd.DataFrame) -> list[dict]:
    """Return one issue dict per column that has missing values.

    Each dict: {column, missing_count, missing_pct (0-100), dtype, sample_values (max 5 non-null)}
    """
    raise NotImplementedError


def suggest_strategy(issue: dict) -> str:
    """Return a cleaning strategy string: 'drop_column' | 'fill_mode' | 'fill_median'."""
    raise NotImplementedError
