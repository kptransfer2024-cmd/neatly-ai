"""Detects columns with identical value sequences — redundant data that wastes memory and confuses analysis."""
import pandas as pd


def detect(df: pd.DataFrame) -> list[dict]:
    """Return one issue dict per group of columns that share identical content."""
    if df.empty or len(df.columns) < 2:
        return []

    # Build a fingerprint per column using a tuple of values (NaN-safe)
    fingerprints: dict[tuple, str] = {}
    issues = []

    for col in df.columns:
        # Use a hashable fingerprint: tuple of (value, isna) pairs
        series = df[col]
        key = (str(series.dtype),) + tuple(zip(series.fillna('__NA__').astype(str), series.isna()))

        if key in fingerprints:
            original = fingerprints[key]
            issues.append({
                'detector': 'duplicate_column_detector',
                'type': 'duplicate_column',
                'column': col,
                'severity': 'medium',
                'row_indices': [],
                'summary': '',
                'sample_data': {
                    col: {
                        'duplicate_of': original,
                        'row_count': len(df),
                    }
                },
                'actions': [
                    {
                        'id': 'drop_duplicate_column',
                        'label': 'Drop Column',
                        'description': f'Remove "{col}" — identical to "{original}"',
                        'params': {'column': col},
                    },
                ],
            })
        else:
            fingerprints[key] = col

    return issues
