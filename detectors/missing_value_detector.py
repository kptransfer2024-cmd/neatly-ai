"""Detects missing / null values in a DataFrame and returns structured issue dicts."""
import pandas as pd


def detect_missing(df: pd.DataFrame) -> list[dict]:
    """Return one issue dict per column that has missing values."""
    issues = []
    total_rows = len(df)
    for col in df.columns:
        missing_count = int(df[col].isna().sum())
        if missing_count == 0:
            continue
        missing_pct = round(missing_count / total_rows * 100, 2)
        sample_values = df[col].dropna().head(5).tolist()
        issues.append({
            'column': col,
            'missing_count': missing_count,
            'missing_pct': missing_pct,
            'dtype': str(df[col].dtype),
            'sample_values': sample_values,
        })
    return issues


def suggest_strategy(issue: dict) -> str:
    """Return a cleaning strategy string based on missing_pct and dtype."""
    if issue['missing_pct'] > 50.0:
        return 'drop_column'
    if issue['dtype'] in ('object', 'str'):
        return 'fill_mode'
    return 'fill_median'
