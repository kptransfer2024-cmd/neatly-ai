"""Wires detectors → explanation_layer → session_state.

Called once per CSV upload to populate st.session_state['issues'].
"""
import pandas as pd
import streamlit as st

from detectors import (
    missing_value_detector,
    duplicate_detector,
    schema_analyzer,
    consistency_cleaner,
    outlier_detector,
)
from explanation_layer import explain_issues


def run_diagnosis(df: pd.DataFrame) -> None:
    """Run all detectors, attach explanations, and write results to session_state.

    Sets st.session_state['issues'] and advances stage to 'decide'.
    """
    raise NotImplementedError


def _collect_df_stats(df: pd.DataFrame) -> dict:
    """Build column-level stats safe to pass to the explanation layer (no raw rows)."""
    raise NotImplementedError
