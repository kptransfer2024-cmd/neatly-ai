"""Detects identifier columns (sequential integers, UUIDs, hex hashes).

ID columns aren't data-quality issues per se, but they are almost always wrong
as model features and often represent PII or sensitive keys. Flagging them
prompts the user to decide: drop, preserve, or pseudonymize.
"""
import re
import numpy as np
import pandas as pd

_MIN_ROWS = 4                 # need a few rows for monotonic / pattern inference
_PATTERN_MATCH_RATE = 0.95    # ≥95% of non-null values must match for string-pattern IDs

_UUID_PATTERN = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
    re.IGNORECASE,
)
_HEX_ID_PATTERN = re.compile(r'^[0-9a-f]{24,}$', re.IGNORECASE)  # Mongo ObjectId (24), SHA-1 (40), SHA-256 (64)


def detect(df: pd.DataFrame) -> list[dict]:
    """Return one issue dict per identifier-like column."""
    if df.empty or len(df.columns) == 0:
        return []

    issues: list[dict] = []
    for col in df.columns:
        series = df[col].dropna()
        if len(series) < _MIN_ROWS:
            continue

        # ID columns must be fully unique (no dupes)
        if series.nunique() != len(series):
            continue

        sub_type = _classify(series)
        if sub_type is None:
            continue

        issues.append(_build_issue(col, sub_type, len(series), len(df)))

    return issues


def _classify(series: pd.Series) -> str | None:
    """Return 'sequential_integer' | 'uuid' | 'hex_id' | None."""
    if pd.api.types.is_integer_dtype(series) or pd.api.types.is_float_dtype(series):
        if _is_sequential_integer(series):
            return 'sequential_integer'
        return None

    if str(series.dtype) in ('object', 'str'):
        as_str = series.astype(str)
        if _matches_pattern(as_str, _UUID_PATTERN):
            return 'uuid'
        if _matches_pattern(as_str, _HEX_ID_PATTERN):
            return 'hex_id'
    return None


def _is_sequential_integer(series: pd.Series) -> bool:
    """True if values are integer-valued and form a consecutive ascending run (step=1)."""
    numeric = pd.to_numeric(series, errors='coerce')
    if numeric.isna().any():
        return False
    # Floats like 1.0, 2.0, 3.0 still qualify if every value is a whole number
    if not np.all(numeric % 1 == 0):
        return False
    sorted_vals = np.sort(numeric.to_numpy().astype('int64'))
    if len(sorted_vals) < _MIN_ROWS:
        return False
    diffs = np.diff(sorted_vals)
    return bool(np.all(diffs == 1))


def _matches_pattern(as_str: pd.Series, pattern: re.Pattern) -> bool:
    match_rate = as_str.str.match(pattern).mean()
    return match_rate >= _PATTERN_MATCH_RATE


def _build_issue(col: str, sub_type: str, unique_count: int, total_rows: int) -> dict:
    return {
        'detector': 'id_column_detector',
        'type': 'id_column',
        'columns': [col],
        'severity': 'medium',
        'row_indices': [],
        'summary': '',
        'sub_type': sub_type,
        'sample_data': {
            col: {
                'sub_type': sub_type,
                'unique_count': unique_count,
                'total_rows': total_rows,
            }
        },
        'actions': [{
            'id': 'drop_column',
            'label': 'Drop Column',
            'description': f'Remove the identifier column "{col}" from the dataset.',
            'params': {'column': col},
        }],
    }
