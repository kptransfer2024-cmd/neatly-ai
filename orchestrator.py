"""Wires detectors → explanation_layer → session_state.

Called once per CSV upload to populate st.session_state['issues'].
"""
import pandas as pd
import streamlit as st

from detectors.missing_value_detector import detect_missing
from detectors.duplicate_detector import detect as detect_duplicates
from detectors.schema_analyzer import detect as detect_schema
from detectors.consistency_cleaner import detect as detect_consistency
from detectors.outlier_detector import detect as detect_outliers
from explanation_layer import explain_issues


def run_diagnosis(df: pd.DataFrame) -> None:
    """Run all detectors, attach explanations, and write results to session_state.

    Sets st.session_state['issues'] and advances stage to 'decide'.
    """
    issues = []

    for detector_fn, detector_name in [
        (detect_missing, 'missing_value'),
        (detect_duplicates, 'duplicates'),
        (detect_schema, 'type_mismatch'),
        (detect_consistency, 'inconsistent_format'),
        (detect_outliers, 'outliers'),
    ]:
        try:
            detector_issues = detector_fn(df)
            if detector_issues:
                for issue in detector_issues:
                    if 'type' not in issue:
                        issue['type'] = detector_name
                    issues.append(issue)
        except Exception:
            pass

    df_stats = _collect_df_stats(df)
    explained_issues = explain_issues(issues, df_stats)

    st.session_state['issues'] = explained_issues
    st.session_state['stage'] = 'decide'


def _collect_df_stats(df: pd.DataFrame) -> dict:
    """Build column-level stats safe to pass to the explanation layer (no raw rows)."""
    if df.empty:
        return {'rows': 0, 'columns': 0}

    stats = {
        'rows': len(df),
        'columns': len(df.columns),
    }

    for col in df.columns:
        series = df[col]
        col_stats = {
            'dtype': str(series.dtype),
            'non_null_count': int(series.notna().sum()),
            'null_count': int(series.isna().sum()),
        }

        if series.dtype in ('float64', 'int64', 'Int64'):
            numeric_vals = pd.to_numeric(series, errors='coerce')
            if numeric_vals.notna().any():
                col_stats['mean'] = float(numeric_vals.mean())
                col_stats['median'] = float(numeric_vals.median())
                col_stats['min'] = float(numeric_vals.min())
                col_stats['max'] = float(numeric_vals.max())
        elif series.dtype in ('object', 'str'):
            try:
                mode_val = series.mode()
                if len(mode_val) > 0:
                    col_stats['mode'] = str(mode_val.iloc[0])
            except (ValueError, IndexError):
                pass

        stats[f'{col}_stats'] = col_stats

    return stats
