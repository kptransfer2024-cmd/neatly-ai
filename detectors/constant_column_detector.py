"""Detects columns with only one distinct value (including all-null columns).

Constant columns carry zero information and typically should be dropped — they
can't explain variance in analysis, don't segment data, and often indicate a
stale feature or an ingestion error.
"""
import pandas as pd


def detect(df: pd.DataFrame) -> list[dict]:
    """Return one issue dict per constant or all-null column."""
    if df.empty or len(df.columns) == 0:
        return []

    issues = []
    total_rows = len(df)
    for col in df.columns:
        series = df[col]
        non_null_count = int(series.notna().sum())

        if non_null_count == 0:
            issues.append(_build_issue(col, 'all_null', None, total_rows, non_null_count))
            continue

        unique_count = int(series.nunique(dropna=True))
        if unique_count == 1:
            constant_value = series.dropna().iloc[0]
            issues.append(_build_issue(col, 'single_value', constant_value, total_rows, non_null_count))

    return issues


def _build_issue(
    col: str,
    sub_type: str,
    constant_value,
    total_rows: int,
    non_null_count: int,
) -> dict:
    sample_data = {
        col: {
            'sub_type': sub_type,
            'non_null_count': non_null_count,
            'total_rows': total_rows,
        }
    }
    if sub_type == 'single_value':
        sample_data[col]['constant_value'] = _coerce_sample(constant_value)

    return {
        'detector': 'constant_column_detector',
        'type': 'constant_column',
        'columns': [col],
        'severity': 'high',
        'row_indices': [],
        'summary': '',
        'sub_type': sub_type,
        'sample_data': sample_data,
        'actions': [{
            'id': 'drop_column',
            'label': 'Drop Column',
            'description': f'Remove the constant column "{col}" from the dataset.',
            'params': {'column': col},
        }],
    }


def _coerce_sample(value):
    """Coerce a pandas/numpy scalar into a JSON-friendly value for sample_data."""
    if hasattr(value, 'item'):
        try:
            return value.item()
        except (ValueError, AttributeError):
            pass
    return value
