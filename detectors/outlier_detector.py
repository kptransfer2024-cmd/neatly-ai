"""Detects statistical outliers in numeric columns using IQR fencing (Tukey)."""
import pandas as pd

_IQR_MULTIPLIER = 1.5  # Tukey's standard: 1.5 = moderate, 3.0 = extreme
_MIN_ROWS_FOR_QUARTILES = 4  # Below this, quartiles are too unstable to trust


def detect(df: pd.DataFrame) -> list[dict]:
    """Return one issue dict per numeric column that has outliers.

    Uses the Tukey fence: outlier if value < Q1 - 1.5*IQR or > Q3 + 1.5*IQR.
    NaN values are ignored (comparisons against NaN return False). Columns
    with fewer than 4 non-null rows or IQR=0 (constant) are skipped.
    """
    numeric = df.select_dtypes(include='number')
    if numeric.empty:
        return []

    # Vectorized quartile computation for all numeric columns in one pass
    q1_all = numeric.quantile(0.25)
    q3_all = numeric.quantile(0.75)

    issues = []
    for col in numeric.columns:
        series = numeric[col]
        if series.count() < _MIN_ROWS_FOR_QUARTILES:
            continue

        q1 = float(q1_all[col])
        q3 = float(q3_all[col])
        iqr = q3 - q1
        if iqr == 0:
            continue

        lower = q1 - _IQR_MULTIPLIER * iqr
        upper = q3 + _IQR_MULTIPLIER * iqr

        values = series.to_numpy()
        outlier_mask = (values < lower) | (values > upper)
        outlier_count = int(outlier_mask.sum())
        if outlier_count == 0:
            continue

        total = len(series)
        issues.append({
            'type': 'outliers',
            'column': col,
            'outlier_count': outlier_count,
            'outlier_pct': round(outlier_count / total * 100, 2),
            'lower_fence': round(lower, 4),
            'upper_fence': round(upper, 4),
            'min_val': float(series.min()),
            'max_val': float(series.max()),
            'sample_indices': outlier_mask.nonzero()[0][:5].tolist(),
        })
    return issues
