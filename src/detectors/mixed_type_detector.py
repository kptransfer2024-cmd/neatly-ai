"""Detects string columns where a majority of values are numeric but some are not — indicating a malformed numeric field."""
import pandas as pd

_MIN_NUMERIC_RATE = 0.6    # ≥60% of non-null values parse as numeric to classify the column
_MIN_DIRTY_COUNT = 2       # suppress noise; require ≥2 non-numeric values


def detect(df: pd.DataFrame) -> list[dict]:
    """Return one issue dict per string column that should be numeric but has dirty values."""
    if df.empty:
        return []

    string_cols = [c for c in df.columns if str(df[c].dtype) in ('object', 'str')]
    issues = []

    for col in string_cols:
        non_null = df[col].dropna()
        if len(non_null) == 0:
            continue

        as_str = non_null.astype(str).str.strip()
        numeric_mask = pd.to_numeric(as_str, errors='coerce').notna()
        numeric_rate = float(numeric_mask.sum() / len(as_str))

        if numeric_rate < _MIN_NUMERIC_RATE:
            continue

        dirty_mask = ~numeric_mask
        dirty_count = int(dirty_mask.sum())

        if dirty_count < _MIN_DIRTY_COUNT:
            continue

        dirty_pct = round(dirty_count / len(non_null) * 100, 2)
        severity = 'high' if dirty_pct > 10 else 'medium' if dirty_pct > 2 else 'low'

        df_positions = df.index[df[col].notna()]
        row_indices = [int(i) for i in df_positions[dirty_mask.to_numpy()][:100]]
        sample_dirty = non_null[dirty_mask].head(5).tolist()

        issues.append({
            'detector': 'mixed_type_detector',
            'type': 'mixed_type',
            'column': col,
            'severity': severity,
            'row_indices': row_indices,
            'summary': '',
            'sample_data': {
                col: {
                    'numeric_rate': round(numeric_rate * 100, 2),
                    'dirty_count': dirty_count,
                    'dirty_pct': dirty_pct,
                    'sample_dirty_values': sample_dirty,
                }
            },
            'actions': [
                {
                    'id': 'coerce_to_numeric',
                    'label': 'Coerce to Numeric',
                    'description': f'Cast "{col}" to float, replacing unparseable values with NaN',
                    'params': {'column': col},
                },
                {
                    'id': 'drop_non_numeric_rows',
                    'label': 'Drop Non-Numeric Rows',
                    'description': f'Remove rows where "{col}" cannot be parsed as a number',
                    'params': {'column': col},
                },
            ],
        })

    return issues
