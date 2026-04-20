"""Detects string columns typed as email/phone/URL/zip and flags malformed values."""
import re
import pandas as pd
from .utils import severity_from_pct, get_string_columns

_MIN_COLUMN_MATCH_RATE = 0.6  # ≥60% of non-null values must match a pattern to "type" the column
_MIN_INVALID_COUNT = 2        # suppress single-value noise; require ≥2 bad values

_PATTERNS = {
    'email': re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$'),
    'us_phone': re.compile(r'^[\+1\-\.\s]*(\(?\d{3}\)?[\-\.\s]?\d{3}[\-\.\s]?\d{4})$'),
    'url': re.compile(r'^https?://[^\s]+$'),
    'us_zip': re.compile(r'^\d{5}(-\d{4})?$'),
}


def detect(df: pd.DataFrame) -> list[dict]:
    """Return one issue dict per (column, pattern) pair where invalids exist."""
    if df.empty:
        return []

    string_cols = get_string_columns(df)
    if not string_cols:
        return []

    issues = []
    for col in string_cols:
        non_null = df[col].dropna()
        if len(non_null) == 0:
            continue

        as_str = non_null.astype(str)
        result = _classify_column(as_str)
        if result is None:
            continue

        pattern_name, match_rate = result
        invalid_count, invalid_mask = _check_pattern(as_str, pattern_name)

        if invalid_count < _MIN_INVALID_COUNT:
            continue

        issue = _build_issue(col, pattern_name, invalid_mask, non_null, df, match_rate)
        issues.append(issue)

    return issues


def _classify_column(series: pd.Series) -> tuple[str, float] | None:
    """Return (pattern_name, match_rate) if series is typed, else None.

    Picks the pattern with the highest match_rate >= _MIN_COLUMN_MATCH_RATE.
    """
    best_name = None
    best_rate = 0.0

    for name, regex in _PATTERNS.items():
        match_mask = series.str.match(regex, na=False)
        match_rate = float(match_mask.sum() / len(series))
        if match_rate >= _MIN_COLUMN_MATCH_RATE and match_rate > best_rate:
            best_name = name
            best_rate = match_rate

    if best_name is None:
        return None
    return (best_name, best_rate)


def _check_pattern(series: pd.Series, pattern_name: str) -> tuple[int, pd.Series]:
    """Return (invalid_count, invalid_mask) for the given compiled pattern."""
    regex = _PATTERNS[pattern_name]
    match_mask = series.str.match(regex, na=False)
    invalid_mask = ~match_mask
    invalid_count = int(invalid_mask.sum())
    return (invalid_count, invalid_mask)


def _build_issue(
    col: str,
    pattern_name: str,
    invalid_mask: pd.Series,
    non_null_series: pd.Series,
    df: pd.DataFrame,
    match_rate: float,
) -> dict:
    """Construct a full issue dict for one (column, pattern_name) pair."""
    invalid_count = int(invalid_mask.sum())
    total_non_null = len(non_null_series)
    invalid_pct = round(invalid_count / total_non_null * 100, 2)

    severity = severity_from_pct(invalid_pct)

    non_null_positions = df[col].notna()
    df_positions = df.index[non_null_positions]
    invalid_positions = df_positions[invalid_mask.to_numpy()]
    row_indices = [int(i) for i in invalid_positions.tolist()]

    sample_invalid = non_null_series[invalid_mask].head(5).tolist()

    return {
        'detector': 'pattern_validator',
        'type': 'pattern_mismatch',
        'columns': [col],
        'severity': severity,
        'row_indices': row_indices,
        'summary': '',
        'sample_data': {
            col: {
                'pattern': pattern_name,
                'invalid_count': invalid_count,
                'invalid_pct': invalid_pct,
                'match_rate': round(match_rate * 100, 2),
            }
        },
        'actions': [
            {
                'id': 'flag_invalid_patterns',
                'label': 'Flag Invalid',
                'description': f'Replace invalid {pattern_name} values with NaN for manual review',
                'params': {'column': col, 'pattern': pattern_name},
            },
            {
                'id': 'drop_invalid_rows',
                'label': 'Drop Invalid Rows',
                'description': f'Remove rows where {col} does not match the {pattern_name} pattern',
                'params': {'column': col, 'pattern': pattern_name},
            },
        ],
    }
