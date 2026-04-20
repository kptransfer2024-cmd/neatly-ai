"""Column header and metrics interpreter — deterministic role, domain, health, and stats inference."""
import pandas as pd
import numpy as np


# ============================================================================
# MODULE-LEVEL CONSTANTS
# ============================================================================

_CONTACT_KEYWORDS = frozenset({
    'email', 'phone', 'mobile', 'fax', 'contact', 'address', 'zip', 'postal'
})
_DATETIME_KEYWORDS = frozenset({
    'date', 'time', 'timestamp', 'created', 'updated', 'dt', 'month', 'day'
})
_ID_KEYWORDS = frozenset({
    'id', 'uuid', 'key', 'code', 'ref', 'no', 'num', 'index', 'pk', 'fk'
})
_FLAG_PREFIXES = ('is_', 'has_')
_FLAG_KEYWORDS = frozenset({
    'flag', 'active', 'enabled', 'deleted', 'verified'
})
_METRIC_KEYWORDS = frozenset({
    'age', 'price', 'cost', 'amount', 'count', 'qty', 'quantity', 'score',
    'rating', 'pct', 'percent', 'rate', 'revenue', 'salary', 'weight',
    'height', 'size', 'length', 'total', 'sum', 'avg', 'mean'
})
_CATEGORY_KEYWORDS = frozenset({
    'status', 'type', 'category', 'group', 'class', 'label',
    'gender', 'country', 'region', 'city', 'state', 'tier'
})

_DOMAIN_BOUNDS = {
    'age': (0.0, 150.0),
    'pct': (0.0, 100.0),
    'percent': (0.0, 100.0),
    'rate': (0.0, 100.0),
    'year': (1900.0, 2100.0),
    'price': (0.0, None),
    'cost': (0.0, None),
    'amount': (0.0, None),
    'count': (0.0, None),
    'qty': (0.0, None),
    'quantity': (0.0, None),
    'score': (0.0, 100.0),
    'rating': (0.0, 5.0),
}

_NULL_WARN_PCT = 5.0
_NULL_BAD_PCT = 20.0
_CARD_ID_RATIO = 95.0
_CARD_CONST_RATIO = 1.0
_CATEGORY_HIGH_UNIQUE_PCT = 50.0


# ============================================================================
# PUBLIC API
# ============================================================================

def build_column_contexts(df: pd.DataFrame) -> list[dict]:
    """Return one context dict per column, in column order.

    Args:
        df: pandas DataFrame

    Returns:
        list[dict]: One context dict per column, each with schema:
            {column, dtype, inferred_role, domain, null_count, null_pct,
             cardinality, unique_pct, health, stats}
    """
    if df.empty:
        return []

    n_rows = len(df)
    return [_build_single_context(col, df[col], n_rows) for col in df.columns]


# ============================================================================
# SINGLE-COLUMN CONTEXT BUILDER
# ============================================================================

def _build_single_context(col_name: str, series: pd.Series, n_rows: int) -> dict:
    """Build a context dict for one column."""
    dtype_str = str(series.dtype)

    # Null stats
    null_count = series.isna().sum()
    null_pct = (null_count / n_rows * 100.0) if n_rows > 0 else 0.0

    # Cardinality
    non_null = series.dropna()
    cardinality = non_null.nunique()
    unique_pct = (cardinality / n_rows * 100.0) if n_rows > 0 else 0.0

    # Role and domain
    inferred_role = _infer_role(col_name, series, cardinality, unique_pct)
    domain = _infer_domain(col_name)

    # Health signal
    health = _infer_health(inferred_role, null_pct, cardinality, unique_pct)

    # Stats
    stats = _compute_stats(series, dtype_str, cardinality, null_count)

    return {
        'column': col_name,
        'dtype': dtype_str,
        'inferred_role': inferred_role,
        'domain': domain,
        'null_count': int(null_count),
        'null_pct': float(null_pct),
        'cardinality': int(cardinality),
        'unique_pct': float(unique_pct),
        'health': health,
        'stats': stats,
    }


# ============================================================================
# ROLE INFERENCE
# ============================================================================

def _infer_role(col_name: str, series: pd.Series, cardinality: int, unique_pct: float) -> str:
    """Infer column role with priority: contact > datetime > flag > id > metric > category > text."""
    col_lower = col_name.lower()
    dtype_str = str(series.dtype)

    # 1. Contact
    if _has_any_keyword(col_lower, _CONTACT_KEYWORDS):
        return 'contact'

    # 2. Datetime
    if 'datetime64' in dtype_str:
        return 'datetime'
    if _has_any_keyword(col_lower, _DATETIME_KEYWORDS):
        return 'datetime'

    # 3. Flag
    if dtype_str == 'bool':
        return 'flag'
    if col_lower.startswith(_FLAG_PREFIXES):
        return 'flag'
    if _has_any_keyword(col_lower, _FLAG_KEYWORDS):
        return 'flag'

    # 4. ID
    if _has_any_keyword(col_lower, _ID_KEYWORDS):
        if unique_pct >= _CARD_ID_RATIO:
            return 'id'

    # 5. Metric
    if _is_numeric(dtype_str):
        return 'metric'
    if _has_any_keyword(col_lower, _METRIC_KEYWORDS):
        return 'metric'

    # 6. Category
    if _is_string(dtype_str):
        if cardinality <= 20:
            return 'category'
    if _has_any_keyword(col_lower, _CATEGORY_KEYWORDS):
        return 'category'

    # 7. Text (fallback for strings)
    if _is_string(dtype_str):
        return 'text'

    return 'text'


def _has_any_keyword(col_lower: str, keywords: frozenset) -> bool:
    """Check if any keyword appears as a word boundary in col_lower."""
    for keyword in keywords:
        if keyword in col_lower:
            return True
    return False


def _is_numeric(dtype_str: str) -> bool:
    """Check if dtype is numeric (int or float)."""
    return 'int' in dtype_str or 'float' in dtype_str or 'Int' in dtype_str or 'Float' in dtype_str


def _is_string(dtype_str: str) -> bool:
    """Check if dtype is string (object or str)."""
    return dtype_str in ('object', 'str')


# ============================================================================
# DOMAIN DETECTION
# ============================================================================

def _infer_domain(col_name: str) -> str | None:
    """Infer domain from column name using fuzzy keyword matching."""
    col_lower = col_name.lower()

    for domain_keyword in _DOMAIN_BOUNDS.keys():
        if domain_keyword in col_lower:
            return domain_keyword

    return None


# ============================================================================
# HEALTH SIGNAL
# ============================================================================

def _infer_health(role: str, null_pct: float, cardinality: int, unique_pct: float) -> str:
    """Infer health signal: 'good' | 'warn' | 'bad'."""
    # Check null first
    if null_pct > _NULL_BAD_PCT:
        return 'bad'
    if null_pct > _NULL_WARN_PCT:
        return 'warn'

    # Check constant columns (very low cardinality)
    if role not in ('flag', 'bool'):
        if unique_pct < _CARD_CONST_RATIO:
            return 'warn'

    # Check category columns with high cardinality
    if role == 'category':
        if unique_pct > _CATEGORY_HIGH_UNIQUE_PCT:
            return 'warn'

    return 'good'


# ============================================================================
# STATS COMPUTATION
# ============================================================================

def _compute_stats(series: pd.Series, dtype_str: str, cardinality: int, null_count: int) -> dict:
    """Compute dtype-specific stats (no raw values in output)."""
    # All-null column
    if cardinality == 0:
        return {}

    if 'int' in dtype_str or 'float' in dtype_str or 'Int' in dtype_str or 'Float' in dtype_str:
        return _numeric_stats(series)
    elif 'datetime64' in dtype_str:
        return _datetime_stats(series)
    elif dtype_str == 'bool':
        return _bool_stats(series)
    elif dtype_str in ('object', 'str'):
        return _string_stats(series)

    return {}


def _numeric_stats(series: pd.Series) -> dict:
    """Return {min, max, mean, median, std}."""
    non_null = series.dropna()
    if len(non_null) == 0:
        return {}

    return {
        'min': float(non_null.min()),
        'max': float(non_null.max()),
        'mean': float(non_null.mean()),
        'median': float(non_null.median()),
        'std': float(non_null.std()),
    }


def _datetime_stats(series: pd.Series) -> dict:
    """Return {min: str, max: str}."""
    non_null = series.dropna()
    if len(non_null) == 0:
        return {}

    return {
        'min': str(non_null.min()),
        'max': str(non_null.max()),
    }


def _bool_stats(series: pd.Series) -> dict:
    """Return {true_pct: float}."""
    non_null = series.dropna()
    if len(non_null) == 0:
        return {}

    true_count = (non_null == True).sum()
    true_pct = (true_count / len(non_null) * 100.0)

    return {
        'true_pct': float(true_pct),
    }


def _string_stats(series: pd.Series) -> dict:
    """Return {mode: str|None, avg_len: float}."""
    non_null = series.dropna()
    if len(non_null) == 0:
        return {}

    # Mode (most frequent value)
    value_counts = non_null.value_counts()
    mode_val = value_counts.index[0] if len(value_counts) > 0 else None

    # Average length
    avg_len = non_null.astype(str).str.len().mean()

    return {
        'mode': mode_val,
        'avg_len': float(avg_len),
    }
