"""Detects columns that could benefit from value standardization.

Flags string columns with mixed casing variants of the same value
(e.g. 'USA', 'usa', 'Usa' in the same column) that suggest the
column lacks a consistent format.
"""
import pandas as pd

_MIN_ROWS = 2
_MIN_DISTINCT = 2


def detect(df: pd.DataFrame) -> list[dict]:
    """Return one issue per column with inconsistent casing."""
    if df.empty or len(df.columns) == 0:
        return []

    issues: list[dict] = []
    for col in df.columns:
        series = df[col]
        if series.dtype not in ('object', 'str', 'string'):
            continue
        non_null = series.dropna().astype(str)
        if len(non_null) < _MIN_ROWS:
            continue

        lower_counts: dict[str, list[str]] = {}
        for val in non_null:
            key = val.strip().lower()
            lower_counts.setdefault(key, [])
            if val not in lower_counts[key]:
                lower_counts[key].append(val)

        # Groups where the same logical value has multiple surface forms
        inconsistent = {k: v for k, v in lower_counts.items() if len(v) >= _MIN_DISTINCT}
        if not inconsistent:
            continue

        example_pairs = list(inconsistent.values())[:3]
        affected = sum(len(v) for v in inconsistent.values())
        total = len(lower_counts)
        pct = affected / total * 100 if total else 0

        issues.append({
            'detector': 'data_standardizer',
            'type': 'standardization_suggested',
            'columns': [col],
            'severity': 'low',
            'row_indices': [],
            'summary': (
                f'"{col}" has {len(inconsistent)} value(s) with inconsistent casing '
                f'(e.g. {example_pairs[0]}). Consider standardizing to a single form.'
            ),
            'sample_data': {col: {'inconsistent_groups': len(inconsistent), 'example_pairs': example_pairs}},
            'actions': [{
                'id': 'standardize_casing',
                'label': 'Standardize to lowercase',
                'description': f'Convert all values in "{col}" to lowercase.',
                'params': {'column': col, 'case': 'lower'},
            }],
        })

    return issues
