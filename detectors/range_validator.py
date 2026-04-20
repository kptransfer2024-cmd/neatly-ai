"""Detects numeric values outside physically/logically valid bounds for known domains."""
import numpy as np
import pandas as pd

_DOMAIN_BOUNDS: dict[str, tuple[float | None, float | None]] = {
    'age': (0.0, 150.0),        # human lifespan upper bound
    'pct': (0.0, 100.0),        # percentage short form
    'percent': (0.0, 100.0),    # percentage long form
    'rate': (0.0, 100.0),       # e.g., tax_rate, success_rate
    'year': (1900.0, 2100.0),   # reasonable published-data range
    'price': (0.0, None),       # no upper bound; negatives are invalid
    'cost': (0.0, None),        # same domain as price
    'amount': (0.0, None),      # generic monetary/quantity
    'count': (0.0, None),       # event counts, inventory
    'qty': (0.0, None),         # quantity abbreviation
    'quantity': (0.0, None),    # quantity full form
    'score': (0.0, 100.0),      # generic score (e.g., test score)
    'rating': (0.0, 5.0),       # typical 5-star rating scale
}

_MIN_VIOLATION_COUNT = 1  # even one impossible value (age=999) is worth surfacing


def detect(df: pd.DataFrame) -> list[dict]:
    """Return one issue dict per numeric column with out-of-range values."""
    if df.empty:
        return []

    numeric = df.select_dtypes(include='number')
    if numeric.empty:
        return []

    issues = []
    for col in numeric.columns:
        domain_info = _infer_domain(col)
        if domain_info is None:
            continue

        keyword, lo, hi = domain_info
        series = numeric[col].dropna()
        if len(series) == 0:
            continue

        violation_mask = _compute_violation_mask(series, lo, hi)
        violation_count = int(violation_mask.sum())

        if violation_count < _MIN_VIOLATION_COUNT:
            continue

        violation_series = series[violation_mask]
        issue = _build_issue(col, keyword, lo, hi, violation_series, df)
        issues.append(issue)

    return issues


def _infer_domain(col_name: str) -> tuple[str, float | None, float | None] | None:
    """Return (keyword, lo, hi) for the first domain keyword found in col_name, else None."""
    lower = col_name.lower()
    for keyword, (lo, hi) in _DOMAIN_BOUNDS.items():
        if keyword in lower:
            return (keyword, lo, hi)
    return None


def _compute_violation_mask(series: pd.Series, lo: float | None, hi: float | None) -> pd.Series:
    """Return boolean Series indicating which values are out of bounds."""
    vals = series.to_numpy(dtype=float)
    lo_mask = (vals < lo) if lo is not None else np.zeros(len(vals), dtype=bool)
    hi_mask = (vals > hi) if hi is not None else np.zeros(len(vals), dtype=bool)
    return pd.Series(lo_mask | hi_mask, index=series.index)


def _build_issue(
    col: str,
    keyword: str,
    lo: float | None,
    hi: float | None,
    violation_series: pd.Series,
    df: pd.DataFrame,
) -> dict:
    """Construct a full issue dict for one column with out-of-range values."""
    violation_count = int(len(violation_series))
    total_non_null = int(df[col].notna().sum())
    violation_pct = round(violation_count / total_non_null * 100, 2)

    severity = 'high' if violation_pct > 20 else 'medium' if violation_pct > 5 else 'low'

    row_indices = [int(i) for i in violation_series.index.tolist()]

    lo_display = float(lo) if lo is not None else None
    hi_display = float(hi) if hi is not None else None

    min_violation = float(violation_series.min())
    max_violation = float(violation_series.max())

    return {
        'detector': 'range_validator',
        'type': 'out_of_range',
        'column': col,
        'severity': severity,
        'row_indices': row_indices,
        'summary': '',
        'sample_data': {
            col: {
                'domain_keyword': keyword,
                'valid_lo': lo_display,
                'valid_hi': hi_display,
                'violation_count': violation_count,
                'violation_pct': violation_pct,
                'min_violation': min_violation,
                'max_violation': max_violation,
            }
        },
        'actions': [
            {
                'id': 'clip_to_range',
                'label': 'Clip to Range',
                'description': f'Clamp values outside [{lo_display}, {hi_display}] to the nearest bound',
                'params': {'column': col, 'lo': lo_display, 'hi': hi_display},
            },
            {
                'id': 'drop_out_of_range_rows',
                'label': 'Drop Invalid Rows',
                'description': f'Remove rows where {col} falls outside [{lo_display}, {hi_display}]',
                'params': {'column': col, 'lo': lo_display, 'hi': hi_display},
            },
        ],
    }
