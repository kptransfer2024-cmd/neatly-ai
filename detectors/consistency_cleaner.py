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


def _build_issue(col: str, sub_type: str, examples: list, severity: str, actions: list) -> dict:
    return {
        'detector': 'consistency_cleaner',
        'type': 'inconsistent_format',
        'columns': [col],
        'severity': severity,
        'row_indices': [],
        'summary': '',
        'sub_type': sub_type,
        'example_values': examples,
        'sample_data': {col: {'sub_type': sub_type, 'examples': examples}},
        'actions': actions,
    }


def _check_extra_whitespace(col: str, as_str: pd.Series, stripped: pd.Series, issues: list) -> None:
    """Flag values with leading/trailing whitespace or internal multi-space runs."""
    ws_mask = (as_str != stripped) | as_str.str.contains(_INTERNAL_WS, regex=True, na=False)
    if not ws_mask.any():
        return
    examples = as_str[ws_mask].head(_EXAMPLE_CAP).tolist()
    actions = [{
        'id': 'normalize_text',
        'label': 'Strip Whitespace',
        'description': f'Strip leading, trailing, and collapsed internal whitespace in "{col}".',
        'params': {'column': col, 'operation': 'strip_whitespace'},
    }]
    issues.append(_build_issue(col, 'extra_whitespace', examples, 'low', actions))


def _check_mixed_case(col: str, stripped: pd.Series, issues: list) -> None:
    """Flag values that collide with each other only when case-normalized."""
    lowered = stripped.str.lower()
    collisions_per_key = stripped.groupby(lowered).nunique()
    colliding_keys = collisions_per_key[collisions_per_key > 1].index
    if len(colliding_keys) == 0:
        return
    collision_mask = lowered.isin(colliding_keys)
    examples = stripped[collision_mask].unique().tolist()[:_EXAMPLE_CAP]
    actions = [
        {
            'id': 'normalize_text',
            'label': 'Lowercase',
            'description': f'Normalize "{col}" to lowercase.',
            'params': {'column': col, 'operation': 'lowercase'},
        },
        {
            'id': 'normalize_text',
            'label': 'Title Case',
            'description': f'Normalize "{col}" to Title Case.',
            'params': {'column': col, 'operation': 'titlecase'},
        },
    ]
    issues.append(_build_issue(col, 'mixed_case', examples, 'low', actions))


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
    examples = examples[:_EXAMPLE_CAP]
    actions = [{
        'id': 'cast_column',
        'label': 'Parse as Date',
        'description': f'Parse "{col}" values as datetime to unify all formats.',
        'params': {'column': col, 'target_dtype': 'datetime'},
    }]
    issues.append(_build_issue(col, 'mixed_date_format', examples, 'medium', actions))
