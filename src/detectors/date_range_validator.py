"""Detects datetime values outside plausible bounds (future dates, pre-1900).

Common CSV pitfalls this catches:
- Excel epoch artifacts (1899-12-30, 1900-01-01) masquerading as real dates
- Unix epoch zeros (1970-01-01) where a missing value was coerced to the epoch
- Typos producing years like 20024 or 19 that parse as valid but absurd dates
- Order dates / created_at / updated_at set in the future due to clock skew

Birth-date hinting: columns whose name contains 'birth', 'dob', or 'born' are
validated against 1900..today (birth in future is impossible). Other datetime
columns use a looser 1900..2100 fence.
"""
from datetime import date
import pandas as pd

_LOOSE_MIN_YEAR = 1900
_LOOSE_MAX_YEAR = 2100
_BIRTH_NAME_HINTS = ('birth', 'dob', 'born')
_MIN_ROWS = 2


def detect(df: pd.DataFrame) -> list[dict]:
    """Return one issue dict per datetime column with suspicious values."""
    if df.empty or len(df.columns) == 0:
        return []

    today = pd.Timestamp(date.today())
    issues: list[dict] = []

    for col in df.columns:
        series = df[col]
        if not pd.api.types.is_datetime64_any_dtype(series):
            continue
        non_null = series.dropna()
        if len(non_null) < _MIN_ROWS:
            continue

        is_birth_like = any(hint in col.lower() for hint in _BIRTH_NAME_HINTS)
        lower = pd.Timestamp(year=_LOOSE_MIN_YEAR, month=1, day=1)
        upper = today if is_birth_like else pd.Timestamp(year=_LOOSE_MAX_YEAR, month=1, day=1)

        below_mask = non_null < lower
        above_mask = non_null > upper
        below_count = int(below_mask.sum())
        above_count = int(above_mask.sum())
        if below_count == 0 and above_count == 0:
            continue

        bad_positions = (below_mask | above_mask)
        bad_indices = non_null.index[bad_positions].tolist()

        total = len(non_null)
        bad_count = below_count + above_count
        issues.append(_build_issue(
            col=col,
            lower=lower,
            upper=upper,
            below_count=below_count,
            above_count=above_count,
            total=total,
            bad_indices=bad_indices,
            is_birth_like=is_birth_like,
        ))
    return issues


def _build_issue(
    *,
    col: str,
    lower: pd.Timestamp,
    upper: pd.Timestamp,
    below_count: int,
    above_count: int,
    total: int,
    bad_indices: list,
    is_birth_like: bool,
) -> dict:
    bad_count = below_count + above_count
    bad_pct = bad_count / total * 100
    severity = _severity(bad_pct)
    sub_type = 'birth_date_out_of_range' if is_birth_like else 'date_out_of_range'

    return {
        'detector': 'date_range_validator',
        'type': 'date_out_of_range',
        'columns': [col],
        'severity': severity,
        'row_indices': bad_indices[:5],
        'summary': '',
        'sub_type': sub_type,
        'sample_data': {
            col: {
                'sub_type': sub_type,
                'valid_lower': lower.isoformat(),
                'valid_upper': upper.isoformat(),
                'before_lower_count': below_count,
                'after_upper_count': above_count,
                'bad_pct': round(bad_pct, 2),
            }
        },
        'actions': [{
            'id': 'drop_out_of_range_dates',
            'label': 'Drop out-of-range rows',
            'description': f'Drop rows whose "{col}" falls outside {lower.date()}..{upper.date()}.',
            'params': {
                'column': col,
                'lower': lower.isoformat(),
                'upper': upper.isoformat(),
            },
        }],
    }


def _severity(bad_pct: float) -> str:
    if bad_pct > 10:
        return 'high'
    if bad_pct > 2:
        return 'medium'
    return 'low'
