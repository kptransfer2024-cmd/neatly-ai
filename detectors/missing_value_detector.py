"""Detects missing / null values in a DataFrame and returns structured issue dicts."""
import pandas as pd

_DROP_THRESHOLD = 50.0
_SKEW_THRESHOLD = 0.5  # |skew| > 0.5 → moderately skewed → median is more robust


def detect_missing(df: pd.DataFrame) -> list[dict]:
    """Return one issue dict per column that has missing values."""
    total_rows = len(df)
    if total_rows == 0:
        return []

    null_counts = df.isnull().sum()
    dtypes = df.dtypes

    issues = []
    for col in null_counts.index[null_counts > 0]:
        missing_count = int(null_counts[col])
        missing_pct = round(missing_count / total_rows * 100, 2)
        sample_values = df[col].dropna().head(5).tolist()

        severity = 'high' if missing_pct > 50 else 'medium' if missing_pct > 10 else 'low'

        issues.append({
            'detector': 'missing_value_detector',
            'column': col,
            'missing_count': missing_count,
            'missing_pct': missing_pct,
            'dtype': str(dtypes[col]),
            'sample_values': sample_values,
            'severity': severity,
            'row_indices': df[df[col].isna()].index.tolist(),
            'summary': '',
            'sample_data': {col: {'missing': missing_count, 'missing_pct': missing_pct}},
            'actions': [
                {
                    'id': 'fill_missing',
                    'label': 'Fill Missing',
                    'description': 'Impute missing values based on data distribution',
                    'params': {'column': col, 'strategy': _get_fill_strategy(df[col])},
                },
                {
                    'id': 'drop_missing',
                    'label': 'Drop Rows',
                    'description': 'Remove rows with missing values',
                    'params': {'columns': [col]},
                },
            ] if missing_pct <= _DROP_THRESHOLD else [
                {
                    'id': 'drop_missing',
                    'label': 'Drop Column',
                    'description': 'Too many missing values—drop the entire column',
                    'params': {'columns': [col]},
                },
            ],
        })
    return issues


def _get_fill_strategy(series: pd.Series) -> str:
    """Determine best imputation strategy based on dtype and distribution."""
    if series.dtype in ('object', 'str'):
        return 'mode'
    clean = series.dropna()
    if len(clean) >= 3 and abs(float(clean.skew())) <= _SKEW_THRESHOLD:
        return 'mean'
    return 'median'


def suggest_strategy(issue: dict, series: pd.Series | None = None) -> str:
    """Return a cleaning strategy string.

    Passes series for skewness-aware imputation: symmetric → fill_mean,
    skewed → fill_median (robust to outliers). Falls back to fill_median
    when series is not provided.
    """
    missing_pct = issue.get('missing_pct', 0)
    if missing_pct > _DROP_THRESHOLD:
        return 'drop_column'

    dtype = issue.get('dtype', '')
    if dtype in ('object', 'str'):
        return 'fill_mode'

    if series is not None:
        clean = series.dropna()
        if len(clean) >= 3 and abs(float(clean.skew())) <= _SKEW_THRESHOLD:
            return 'fill_mean'
    return 'fill_median'
