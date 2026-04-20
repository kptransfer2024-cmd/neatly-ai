"""Detects statistical outliers in numeric columns using IQR fencing (Tukey)."""
import pandas as pd

_IQR_MULTIPLIER = 1.5  # Tukey's standard: 1.5 = moderate, 3.0 = extreme
_MIN_ROWS_FOR_QUARTILES = 4  # Below this, quartiles are too unstable to trust
_SEVERITY_HIGH_PCT = 10.0
_SEVERITY_MED_PCT = 2.0


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

        total_non_null = series.count()
        outlier_pct = round(outlier_count / total_non_null * 100, 2)
        row_indices = outlier_mask.nonzero()[0][:100].tolist()
        lower_r = round(lower, 4)
        upper_r = round(upper, 4)
        min_v = float(series.min())
        max_v = float(series.max())

        if outlier_pct > _SEVERITY_HIGH_PCT:
            severity = 'high'
        elif outlier_pct > _SEVERITY_MED_PCT:
            severity = 'medium'
        else:
            severity = 'low'

        issues.append({
            'detector': 'outlier_detector',
            'type': 'outliers',
            'columns': [col],
            'severity': severity,
            'row_indices': row_indices,
            'summary': '',
            'outlier_count': outlier_count,
            'outlier_pct': outlier_pct,
            'lower_fence': lower_r,
            'upper_fence': upper_r,
            'min_val': min_v,
            'max_val': max_v,
            'sample_data': {
                col: {
                    'outlier_count': outlier_count,
                    'outlier_pct': outlier_pct,
                    'lower_fence': lower_r,
                    'upper_fence': upper_r,
                    'min_val': min_v,
                    'max_val': max_v,
                },
            },
            'actions': [
                {
                    'id': 'clip_outliers',
                    'label': 'Clip to Fence',
                    'description': f'Clip values outside [{lower_r}, {upper_r}] to the fence bounds.',
                    'params': {'column': col, 'lower': lower_r, 'upper': upper_r},
                },
                {
                    'id': 'drop_rows',
                    'label': 'Drop Outlier Rows',
                    'description': f'Remove {outlier_count} row(s) with outlier values.',
                    'params': {'column': col, 'row_indices': row_indices},
                },
            ],
        })
    return issues
