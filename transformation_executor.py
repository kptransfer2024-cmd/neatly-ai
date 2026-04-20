"""Applies deterministic pandas transforms and appends entries to cleaning_log.

RULE: Every public function here must append to st.session_state['cleaning_log'].
RULE: No LLM calls — pure pandas/Python only.
"""
import pandas as pd
import streamlit as st


def drop_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate rows and log the action."""
    raise NotImplementedError


def fill_missing(df: pd.DataFrame, column: str, strategy: str, fill_value=None) -> pd.DataFrame:
    """Fill missing values in *column* using *strategy* ('mean'|'median'|'mode'|'constant').

    Appends to cleaning_log with column, strategy, and count of filled cells.
    """
    raise NotImplementedError


def drop_missing(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Drop rows where *column* is null and log the action."""
    raise NotImplementedError


def cast_column(df: pd.DataFrame, column: str, target_dtype: str) -> pd.DataFrame:
    """Cast *column* to *target_dtype* and log the action."""
    raise NotImplementedError


def normalize_text(df: pd.DataFrame, column: str, operation: str) -> pd.DataFrame:
    """Apply text normalization to *column*.

    operation: 'strip_whitespace' | 'lowercase' | 'uppercase' | 'titlecase'
    """
    raise NotImplementedError


def clip_outliers(df: pd.DataFrame, column: str, lower: float, upper: float) -> pd.DataFrame:
    """Clip values in *column* to [lower, upper] and log the action."""
    raise NotImplementedError


def _log(action: str, details: dict) -> None:
    """Append a log entry to st.session_state['cleaning_log']."""
    st.session_state['cleaning_log'].append({'action': action, **details})
