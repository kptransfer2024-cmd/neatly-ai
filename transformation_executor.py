"""Applies deterministic pandas transforms and appends entries to cleaning_log.

RULE: Every public function appends a dict to the caller's cleaning_log list.
RULE: No LLM calls — pure pandas/Python only.

The cleaning_log parameter is a plain list — the Streamlit layer passes
st.session_state['cleaning_log']; tests pass [] and inspect the result.
"""
import pandas as pd


def drop_duplicates(df: pd.DataFrame, cleaning_log: list) -> pd.DataFrame:
    """Remove duplicate rows and log the action."""
    before_count = len(df)
    result = df.drop_duplicates(keep='first').reset_index(drop=True)
    after_count = len(result)

    if before_count > after_count:
        _log(cleaning_log, 'drop_duplicates', {
            'row_count_before': before_count,
            'row_count_after': after_count,
            'duplicates_removed': before_count - after_count,
        })

    return result


def fill_missing(
    df: pd.DataFrame,
    column: str,
    strategy: str,
    cleaning_log: list,
    fill_value=None,
) -> pd.DataFrame:
    """Fill missing values in *column* using *strategy* ('mean'|'median'|'mode'|'constant')."""
    if column not in df.columns:
        raise KeyError(f"Column '{column}' not found in DataFrame")

    result = df.copy()
    missing_count = int(result[column].isna().sum())
    if missing_count == 0:
        return result

    if strategy == 'mean':
        fill_val = result[column].mean()
        result[column] = result[column].fillna(fill_val)
    elif strategy == 'median':
        fill_val = result[column].median()
        result[column] = result[column].fillna(fill_val)
    elif strategy == 'mode':
        modes = result[column].mode()
        if len(modes) == 0:
            return result
        fill_val = modes.iloc[0]
        result[column] = result[column].fillna(fill_val)
    elif strategy == 'constant':
        if fill_value is None:
            raise ValueError("fill_value required for 'constant' strategy")
        fill_val = fill_value
        result[column] = result[column].fillna(fill_value)
    else:
        raise ValueError(f"Unknown strategy: {strategy}")

    _log(cleaning_log, 'fill_missing', {
        'column': column,
        'strategy': strategy,
        'filled_count': missing_count,
        'fill_value': _coerce_for_log(fill_val),
    })
    return result


def drop_missing(df: pd.DataFrame, column: str, cleaning_log: list) -> pd.DataFrame:
    """Drop rows where *column* is null and log the action."""
    if column not in df.columns:
        raise KeyError(f"Column '{column}' not found in DataFrame")

    before_count = len(df)
    result = df.dropna(subset=[column]).reset_index(drop=True)
    after_count = len(result)

    if before_count > after_count:
        _log(cleaning_log, 'drop_missing', {
            'column': column,
            'row_count_before': before_count,
            'row_count_after': after_count,
            'rows_dropped': before_count - after_count,
        })
    return result


def cast_column(
    df: pd.DataFrame,
    column: str,
    target_dtype: str,
    cleaning_log: list,
) -> pd.DataFrame:
    """Cast *column* to *target_dtype* and log the action.

    Supported target_dtype values: 'int', 'float', 'str', 'bool', 'datetime',
    or any pandas dtype string accepted by astype.
    """
    if column not in df.columns:
        raise KeyError(f"Column '{column}' not found in DataFrame")

    result = df.copy()
    original_dtype = str(result[column].dtype)

    try:
        if target_dtype == 'int':
            result[column] = pd.to_numeric(result[column], errors='coerce').astype('Int64')
        elif target_dtype == 'float':
            result[column] = pd.to_numeric(result[column], errors='coerce').astype('float64')
        elif target_dtype == 'datetime':
            result[column] = pd.to_datetime(result[column], errors='coerce')
        elif target_dtype == 'str':
            result[column] = result[column].astype('str')
        elif target_dtype == 'bool':
            result[column] = result[column].astype('bool')
        else:
            result[column] = result[column].astype(target_dtype)
    except (ValueError, TypeError) as e:
        raise ValueError(f"Cannot cast column '{column}' to {target_dtype}: {e}") from e

    new_dtype = str(result[column].dtype)
    if original_dtype != new_dtype:
        _log(cleaning_log, 'cast_column', {
            'column': column,
            'from_dtype': original_dtype,
            'to_dtype': new_dtype,
        })
    return result


def normalize_text(
    df: pd.DataFrame,
    column: str,
    operation: str,
    cleaning_log: list,
) -> pd.DataFrame:
    """Apply text normalization to *column*.

    operation: 'strip_whitespace' | 'lowercase' | 'uppercase' | 'titlecase'
    """
    if column not in df.columns:
        raise KeyError(f"Column '{column}' not found in DataFrame")

    result = df.copy()
    if operation == 'strip_whitespace':
        result[column] = result[column].str.strip()
    elif operation == 'lowercase':
        result[column] = result[column].str.lower()
    elif operation == 'uppercase':
        result[column] = result[column].str.upper()
    elif operation == 'titlecase':
        result[column] = result[column].str.title()
    else:
        raise ValueError(f"Unknown operation: {operation}")

    _log(cleaning_log, 'normalize_text', {
        'column': column,
        'operation': operation,
    })
    return result


def clip_outliers(
    df: pd.DataFrame,
    column: str,
    lower: float,
    upper: float,
    cleaning_log: list,
) -> pd.DataFrame:
    """Clip values in *column* to [lower, upper] and log the action."""
    if column not in df.columns:
        raise KeyError(f"Column '{column}' not found in DataFrame")

    result = df.copy()
    series = result[column]
    before_min = float(series.min()) if series.notna().any() else None
    before_max = float(series.max()) if series.notna().any() else None

    clipped_count = int(((series < lower) | (series > upper)).sum())
    result[column] = series.clip(lower, upper)

    if clipped_count > 0:
        _log(cleaning_log, 'clip_outliers', {
            'column': column,
            'lower_bound': lower,
            'upper_bound': upper,
            'clipped_count': clipped_count,
            'value_range_before': [before_min, before_max],
        })
    return result


def merge_near_duplicates(
    df: pd.DataFrame,
    cleaning_log: list,
    column: str,
    row_indices: list,
) -> pd.DataFrame:
    """Drop all but first row in a near-duplicate cluster and log the action."""
    if not row_indices or len(row_indices) < 2:
        return df

    result = df.copy()
    dropped_indices = set(row_indices[1:])
    result = result.drop(index=dropped_indices).reset_index(drop=True)

    _log(cleaning_log, 'merge_near_duplicates', {
        'column': column,
        'rows_merged': len(dropped_indices),
        'sample_indices': row_indices[:3],
    })

    return result


def flag_near_duplicates(
    df: pd.DataFrame,
    cleaning_log: list,
    column: str,
    row_indices: list,
) -> pd.DataFrame:
    """Add a boolean flag column marking near-duplicate rows and log the action."""
    result = df.copy()
    flag_col = f'{column}_near_duplicate_flag'
    result[flag_col] = False
    result.loc[row_indices, flag_col] = True

    _log(cleaning_log, 'flag_near_duplicates', {
        'column': column,
        'flag_column': flag_col,
        'flagged_count': len(row_indices),
    })

    return result


def flag_invalid_patterns(
    df: pd.DataFrame,
    cleaning_log: list,
    column: str,
    pattern: str,
) -> pd.DataFrame:
    """Replace values not matching the pattern with NaN and log the action."""
    import re

    pattern_regexes = {
        'email': r'^[^@\s]+@[^@\s]+\.[^@\s]+$',
        'us_phone': r'^[\+1\-\.\s]*(\(?\d{3}\)?[\-\.\s]?\d{3}[\-\.\s]?\d{4})$',
        'url': r'^https?://[^\s]+$',
        'us_zip': r'^\d{5}(-\d{4})?$',
    }

    if pattern not in pattern_regexes:
        raise ValueError(f"Unknown pattern: {pattern}")

    result = df.copy()
    regex = re.compile(pattern_regexes[pattern])
    mask = result[column].astype(str).str.match(regex, na=False)
    invalid_count = (~mask).sum()

    result.loc[~mask, column] = None

    _log(cleaning_log, 'flag_invalid_patterns', {
        'column': column,
        'pattern': pattern,
        'flagged_as_null_count': int(invalid_count),
    })

    return result


def drop_invalid_rows(
    df: pd.DataFrame,
    cleaning_log: list,
    column: str,
    pattern: str,
) -> pd.DataFrame:
    """Drop rows where column doesn't match the pattern and log the action."""
    import re

    pattern_regexes = {
        'email': r'^[^@\s]+@[^@\s]+\.[^@\s]+$',
        'us_phone': r'^[\+1\-\.\s]*(\(?\d{3}\)?[\-\.\s]?\d{3}[\-\.\s]?\d{4})$',
        'url': r'^https?://[^\s]+$',
        'us_zip': r'^\d{5}(-\d{4})?$',
    }

    if pattern not in pattern_regexes:
        raise ValueError(f"Unknown pattern: {pattern}")

    before_count = len(df)
    regex = re.compile(pattern_regexes[pattern])
    mask = df[column].astype(str).str.match(regex, na=False)
    result = df[mask].reset_index(drop=True)
    after_count = len(result)

    _log(cleaning_log, 'drop_invalid_rows', {
        'column': column,
        'pattern': pattern,
        'rows_dropped': before_count - after_count,
    })

    return result


def drop_out_of_range_rows(
    df: pd.DataFrame,
    cleaning_log: list,
    column: str,
    lo: float | None,
    hi: float | None,
) -> pd.DataFrame:
    """Drop rows where column values fall outside [lo, hi] range and log the action."""
    before_count = len(df)
    result = df.copy()

    if lo is not None:
        result = result[result[column] >= lo]
    if hi is not None:
        result = result[result[column] <= hi]

    result = result.reset_index(drop=True)
    after_count = len(result)

    _log(cleaning_log, 'drop_out_of_range_rows', {
        'column': column,
        'valid_range': [lo, hi],
        'rows_dropped': before_count - after_count,
    })

    return result


def _log(cleaning_log: list, action: str, details: dict) -> None:
    """Append one log entry to the caller's cleaning_log list."""
    cleaning_log.append({'action': action, **details})


def _coerce_for_log(value):
    """Convert pandas/numpy scalar to a JSON-friendly value for logging."""
    if value is None:
        return None
    if hasattr(value, 'item'):
        try:
            return value.item()
        except (ValueError, AttributeError):
            pass
    return value
