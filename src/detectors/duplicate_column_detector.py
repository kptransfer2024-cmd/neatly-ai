"""Detects columns with identical value sequences — redundant data that wastes memory and confuses analysis."""
import pandas as pd


def detect(df: pd.DataFrame) -> list[dict]:
    """Return one issue dict per group of columns that share identical content."""
    if df.empty or len(df.columns) < 2:
        return []

    # Build a fingerprint per column using vectorized hashing (memory-efficient)
    fingerprints: dict[tuple, str] = {}
    issues = []

    for col in df.columns:
        series = df[col]

        # Compute hash of the series values and null mask combined.
        # pd.util.hash_pandas_object is much faster and uses less memory than tuple(zip(...))
        value_hash = pd.util.hash_pandas_object(series, index=False).sum()
        null_mask_hash = pd.util.hash_pandas_object(series.isna(), index=False).sum()

        # Combine dtype, value hash, and null hash into a single key
        # (dtype is included to prevent false positives from different types)
        key = (str(series.dtype), value_hash, null_mask_hash)

        if key in fingerprints:
            original = fingerprints[key]
            issues.append({
                'detector': 'duplicate_column_detector',
                'type': 'duplicate_column',
                'columns': [col],
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
