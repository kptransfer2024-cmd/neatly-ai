"""Applies deterministic pandas transforms and appends entries to cleaning_log.

RULE: Every public function here must append to st.session_state['cleaning_log'].
RULE: No LLM calls — pure pandas/Python only.
"""
import pandas as pd
import streamlit as st


def drop_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate rows and log the action."""
    before_count = len(df)
    result = df.drop_duplicates(keep='first').reset_index(drop=True)
    after_count = len(result)

    if before_count > after_count:
        _log('drop_duplicates', {
            'row_count_before': before_count,
            'row_count_after': after_count,
            'duplicates_removed': before_count - after_count,
        })

    return result


def fill_missing(df: pd.DataFrame, column: str, strategy: str, fill_value=None) -> pd.DataFrame:
    """Fill missing values in *column* using *strategy* ('mean'|'median'|'mode'|'constant').

    Appends to cleaning_log with column, strategy, and count of filled cells.
    """
    if column not in df.columns:
        raise KeyError(f"Column '{column}' not found in DataFrame")

    result = df.copy()
    missing_count = result[column].isna().sum()

    if missing_count == 0:
        return result

    if strategy == 'mean':
        fill_val = result[column].mean()
        result[column] = result[column].fillna(fill_val)
    elif strategy == 'median':
        fill_val = result[column].median()
        result[column] = result[column].fillna(fill_val)
    elif strategy == 'mode':
        fill_val = result[column].mode()
        if len(fill_val) > 0:
            result[column] = result[column].fillna(fill_val[0])
    elif strategy == 'constant':
        if fill_value is None:
            raise ValueError("fill_value required for 'constant' strategy")
        result[column] = result[column].fillna(fill_value)
    else:
        raise ValueError(f"Unknown strategy: {strategy}")

    _log('fill_missing', {
        'column': column,
        'strategy': strategy,
        'filled_count': missing_count,
    })

    return result


def drop_missing(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Drop rows where *column* is null and log the action."""
    if column not in df.columns:
        raise KeyError(f"Column '{column}' not found in DataFrame")

    before_count = len(df)
    result = df.dropna(subset=[column]).reset_index(drop=True)
    after_count = len(result)

    if before_count > after_count:
        _log('drop_missing', {
            'column': column,
            'row_count_before': before_count,
            'row_count_after': after_count,
            'rows_dropped': before_count - after_count,
        })

    return result


def cast_column(df: pd.DataFrame, column: str, target_dtype: str) -> pd.DataFrame:
    """Cast *column* to *target_dtype* and log the action."""
    if column not in df.columns:
        raise KeyError(f"Column '{column}' not found in DataFrame")

    result = df.copy()
    original_dtype = str(result[column].dtype)

    try:
        if target_dtype == 'int':
            result[column] = result[column].astype('Int64')  # nullable int
        elif target_dtype == 'float':
            result[column] = result[column].astype('float64')
        elif target_dtype == 'str':
            result[column] = result[column].astype('str')
        elif target_dtype == 'bool':
            result[column] = result[column].astype('bool')
        else:
            result[column] = result[column].astype(target_dtype)
    except (ValueError, TypeError) as e:
        raise ValueError(f"Cannot cast column '{column}' to {target_dtype}: {e}")

    new_dtype = str(result[column].dtype)

    if original_dtype != new_dtype:
        _log('cast_column', {
            'column': column,
            'from_dtype': original_dtype,
            'to_dtype': new_dtype,
        })

    return result


def normalize_text(df: pd.DataFrame, column: str, operation: str) -> pd.DataFrame:
    """Apply text normalization to *column*.

    operation: 'strip_whitespace' | 'lowercase' | 'uppercase' | 'titlecase'
    """
    if column not in df.columns:
        raise KeyError(f"Column '{column}' not found in DataFrame")

    result = df.copy()
    original_count = result[column].dtype == object or result[column].dtype == 'str'

    if operation == 'strip_whitespace':
        result[column] = result[column].str.strip()
        op_name = 'strip_whitespace'
    elif operation == 'lowercase':
        result[column] = result[column].str.lower()
        op_name = 'lowercase'
    elif operation == 'uppercase':
        result[column] = result[column].str.upper()
        op_name = 'uppercase'
    elif operation == 'titlecase':
        result[column] = result[column].str.title()
        op_name = 'titlecase'
    else:
        raise ValueError(f"Unknown operation: {operation}")

    _log('normalize_text', {
        'column': column,
        'operation': op_name,
    })

    return result


def clip_outliers(df: pd.DataFrame, column: str, lower: float, upper: float) -> pd.DataFrame:
    """Clip values in *column* to [lower, upper] and log the action."""
    if column not in df.columns:
        raise KeyError(f"Column '{column}' not found in DataFrame")

    result = df.copy()
    before_min = result[column].min()
    before_max = result[column].max()

    clipped_count = ((result[column] < lower) | (result[column] > upper)).sum()
    result[column] = result[column].clip(lower, upper)

    if clipped_count > 0:
        _log('clip_outliers', {
            'column': column,
            'lower_bound': lower,
            'upper_bound': upper,
            'clipped_count': clipped_count,
            'value_range_before': [float(before_min), float(before_max)],
        })

    return result


def _log(action: str, details: dict) -> None:
    """Append a log entry to st.session_state['cleaning_log']."""
    st.session_state['cleaning_log'].append({'action': action, **details})
