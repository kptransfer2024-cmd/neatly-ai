"""Detects missing / null values in a DataFrame and returns structured issue dicts."""
import pandas as pd

_DROP_THRESHOLD = 50.0
_SKEW_THRESHOLD = 0.5  # |skew| > 0.5 → moderately skewed → median is more robust


def detect_missing(df: pd.DataFrame) -> list[dict]:
    """Return one issue dict per column that has missing values."""
    total_rows = len(df)
    if total_rows == 0:
        return []

    # Single vectorized pass over the entire frame instead of N per-column passes
    null_counts = df.isnull().sum()
    dtypes = df.dtypes

    issues = []
    for col in null_counts.index[null_counts > 0]:
        missing_count = int(null_counts[col])
        issues.append({
            'column': col,
            'missing_count': missing_count,
            'missing_pct': round(missing_count / total_rows * 100, 2),
            'dtype': str(dtypes[col]),
            'sample_values': df[col].dropna().head(5).tolist(),
        })
    return issues


def suggest_strategy(issue: dict, series: pd.Series | None = None) -> str:
    """Return a cleaning strategy string.

    Passes series for skewness-aware imputation: symmetric → fill_mean,
    skewed → fill_median (robust to outliers). Falls back to fill_median
    when series is not provided.
    """
    if issue['missing_pct'] > _DROP_THRESHOLD:
        return 'drop_column'
    if issue['dtype'] in ('object', 'str'):
        return 'fill_mode'
    if series is not None:
        clean = series.dropna()
        if len(clean) >= 3 and abs(float(clean.skew())) <= _SKEW_THRESHOLD:
            return 'fill_mean'
    return 'fill_median'
