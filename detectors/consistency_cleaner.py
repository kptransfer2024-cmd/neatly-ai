"""Detects inconsistent formatting within columns (mixed case, extra whitespace, mixed date formats)."""
import re
import pandas as pd

_EXAMPLE_CAP = 5
_DATE_FORMAT_THRESHOLD = 0.8  # ≥80% of values match some date pattern → treat column as dates
_DATE_PATTERNS = {
    'iso':       re.compile(r'^\d{4}-\d{1,2}-\d{1,2}$'),      # 2024-01-15
    'slash':     re.compile(r'^\d{1,2}/\d{1,2}/\d{2,4}$'),    # 01/15/2024 or 1/15/24
    'dot':       re.compile(r'^\d{1,2}\.\d{1,2}\.\d{2,4}$'),  # 15.01.2024
    'dash_dmy':  re.compile(r'^\d{1,2}-\d{1,2}-\d{4}$'),      # 15-01-2024
}
_INTERNAL_WS = re.compile(r'\s{2,}')


def detect(df: pd.DataFrame) -> list[dict]:
    """Return one issue dict per (column, sub_type) with detected formatting inconsistencies.

    Issue dict shape:
        {
            'type': 'inconsistent_format',
            'column': str,
            'sub_type': str,   # 'mixed_case' | 'extra_whitespace' | 'mixed_date_format'
            'example_values': list,
        }
    """
    issues = []
    for col in df.columns:
        if str(df[col].dtype) not in ('object', 'str'):
            continue

        non_null = df[col].dropna()
        if len(non_null) == 0:
            continue

        as_str = non_null.astype(str)
        stripped = as_str.str.strip()

        _check_extra_whitespace(col, as_str, stripped, issues)
        _check_mixed_case(col, stripped, issues)
        _check_mixed_date_format(col, stripped, issues)

    return issues


def _check_extra_whitespace(col: str, as_str: pd.Series, stripped: pd.Series, issues: list) -> None:
    """Flag values with leading/trailing whitespace or internal multi-space runs."""
    ws_mask = (as_str != stripped) | as_str.str.contains(_INTERNAL_WS, regex=True, na=False)
    if not ws_mask.any():
        return
    issues.append({
        'type': 'inconsistent_format',
        'column': col,
        'sub_type': 'extra_whitespace',
        'example_values': as_str[ws_mask].head(_EXAMPLE_CAP).tolist(),
    })


def _check_mixed_case(col: str, stripped: pd.Series, issues: list) -> None:
    """Flag values that collide with each other only when case-normalized."""
    lowered = stripped.str.lower()
    # Values whose lowercased form has >1 distinct original casing = collision
    collisions_per_key = stripped.groupby(lowered).nunique()
    colliding_keys = collisions_per_key[collisions_per_key > 1].index
    if len(colliding_keys) == 0:
        return
    collision_mask = lowered.isin(colliding_keys)
    examples = stripped[collision_mask].unique().tolist()[:_EXAMPLE_CAP]
    issues.append({
        'type': 'inconsistent_format',
        'column': col,
        'sub_type': 'mixed_case',
        'example_values': examples,
    })


def _check_mixed_date_format(col: str, stripped: pd.Series, issues: list) -> None:
    """Flag columns that are mostly date-like but use multiple format styles."""
    format_hits: dict[str, pd.Series] = {}
    for name, pattern in _DATE_PATTERNS.items():
        mask = stripped.str.match(pattern, na=False)
        if mask.any():
            format_hits[name] = mask

    if len(format_hits) < 2:
        return
    total_matched = sum(int(m.sum()) for m in format_hits.values())
    if total_matched / len(stripped) < _DATE_FORMAT_THRESHOLD:
        return

    examples: list[str] = []
    for mask in format_hits.values():
        examples.extend(stripped[mask].head(2).tolist())
    issues.append({
        'type': 'inconsistent_format',
        'column': col,
        'sub_type': 'mixed_date_format',
        'example_values': examples[:_EXAMPLE_CAP],
    })
