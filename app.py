"""Streamlit entry point.

Manages stage transitions and renders the appropriate view for each stage.
Reads and writes st.session_state only — no business logic here.
"""
import json

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from orchestrator import run_diagnosis
from transformation_executor import (
    cast_column,
    clip_outliers,
    drop_duplicates,
    drop_invalid_rows,
    drop_missing,
    drop_out_of_range_rows,
    fill_missing,
    flag_invalid_patterns,
    flag_near_duplicates,
    merge_near_duplicates,
    normalize_text,
)
from utils.file_ingestion import parse_uploaded_file
from utils.session_state import init_state

load_dotenv()

# ---------------------------------------------------------------------------
# Session state initialisation
# ---------------------------------------------------------------------------

init_state()

# ---------------------------------------------------------------------------
# Stage: upload
# ---------------------------------------------------------------------------

def render_upload() -> None:
    st.header('Upload your CSV')
    st.write("Neatly will find data-quality issues, explain them in plain English, and let you clean the file with a few clicks.")
    uploaded_file = st.file_uploader('Choose a CSV file', type='csv')

    if not uploaded_file:
        return
    try:
        df = parse_uploaded_file(uploaded_file)
    except Exception as e:
        st.error(f'Error reading file: {e}')
        return

    st.success(f'Loaded {len(df):,} rows, {len(df.columns)} columns')
    st.dataframe(df.head(10), use_container_width=True)

    if st.button('Start Diagnosis', key='diagnose_btn', type='primary'):
        st.session_state['original_df'] = df.copy()
        st.session_state['df'] = df
        st.session_state['issues'] = []
        st.session_state['cleaning_log'] = []
        st.session_state['stage'] = 'diagnose'
        st.rerun()

# ---------------------------------------------------------------------------
# Stage: diagnose
# ---------------------------------------------------------------------------

def render_diagnose() -> None:
    st.header('Diagnosing your data…')
    with st.spinner('Running detectors and generating explanations…'):
        result = run_diagnosis(st.session_state['df'])
        st.session_state['issues'] = result['issues']
        st.session_state['stage'] = 'decide'
    st.rerun()

# ---------------------------------------------------------------------------
# Stage: decide
# ---------------------------------------------------------------------------

_STATS_HIDE_KEYS = {
    'type', 'column', 'columns', 'sub_type', 'explanation', 'summary',
    'sample_values', 'example_values', 'sample_indices', 'row_indices',
    'dtype', 'detector', 'severity', 'sample_data', 'actions',
}


def render_decide() -> None:
    st.header('Review & Fix Issues')
    issues = st.session_state['issues']
    if not issues:
        st.info('No issues found — your data looks clean.')
        if st.button('Finish', key='finish_btn', type='primary'):
            st.session_state['stage'] = 'done'
            st.rerun()
        return

    st.caption(f"{len(issues)} issue(s) remaining.")
    for i, issue in enumerate(issues):
        _render_issue_card(i, issue)

    st.divider()
    col1, col2 = st.columns(2)
    if col1.button('Done Reviewing', key='done_review_btn', type='primary'):
        st.session_state['stage'] = 'done'
        st.rerun()
    if col2.button('Start Over', key='restart_decide_btn'):
        _reset_to_upload()
        st.rerun()


def _render_issue_card(idx: int, issue: dict) -> None:
    issue_type = issue.get('type', 'issue')
    columns = issue.get('columns') or ([issue['column']] if issue.get('column') else [])
    column = columns[0] if columns else None
    title = _humanize(issue_type) + (f" — `{', '.join(columns)}`" if columns else "")
    with st.container(border=True):
        st.subheader(title)
        explanation = issue.get('explanation') or issue.get('summary')
        if explanation:
            st.write(explanation)
        stats = {k: v for k, v in issue.items() if k not in _STATS_HIDE_KEYS}
        if stats:
            st.caption(" • ".join(f"{k}: {v}" for k, v in stats.items()))

        actions = _actions_for(issue)
        if not actions:
            st.caption("_No automatic fix available for this issue._")
            return
        cols = st.columns(len(actions) + 1)
        for btn_col, (label, handler) in zip(cols, actions):
            if btn_col.button(label, key=f'act_{idx}_{label}'):
                _apply_action(idx, handler)
        if cols[-1].button('Skip', key=f'skip_{idx}'):
            _dismiss_issue(idx)


def _actions_for(issue: dict) -> list[tuple[str, callable]]:
    """Map an issue to (button_label, handler(df, log) -> df) pairs."""
    issue_type = issue.get('type')
    cols = issue.get('columns') or ([issue['column']] if issue.get('column') else [])
    col = cols[0] if cols else None

    if issue_type == 'missing_value' and col:
        dtype = issue.get('dtype', '')
        actions: list[tuple[str, callable]] = []
        if dtype not in ('object', 'str'):
            actions.append(('Fill mean', lambda df, log: fill_missing(df, col, 'mean', log)))
            actions.append(('Fill median', lambda df, log: fill_missing(df, col, 'median', log)))
        actions.append(('Fill mode', lambda df, log: fill_missing(df, col, 'mode', log)))
        actions.append(('Drop rows', lambda df, log: drop_missing(df, col, log)))
        return actions

    if issue_type == 'duplicates':
        return [('Drop duplicate rows', lambda df, log: drop_duplicates(df, log))]

    if issue_type == 'outliers' and col:
        lower, upper = issue.get('lower_fence'), issue.get('upper_fence')
        if lower is not None and upper is not None:
            return [('Clip to IQR fence', lambda df, log: clip_outliers(df, col, lower, upper, log))]

    if issue_type == 'type_mismatch' and col:
        target = issue.get('suggested_dtype')
        if target:
            return [(f'Cast to {target}', lambda df, log: cast_column(df, col, target, log))]

    if issue_type == 'inconsistent_format' and col:
        sub = issue.get('sub_type')
        if sub == 'extra_whitespace':
            return [('Strip whitespace', lambda df, log: normalize_text(df, col, 'strip_whitespace', log))]
        if sub == 'mixed_case':
            return [
                ('Lowercase', lambda df, log: normalize_text(df, col, 'lowercase', log)),
                ('Titlecase', lambda df, log: normalize_text(df, col, 'titlecase', log)),
            ]
        if sub == 'mixed_date_format':
            return [('Cast to datetime', lambda df, log: cast_column(df, col, 'datetime', log))]

    if issue_type == 'near_duplicates' and col:
        row_indices = issue.get('row_indices', [])
        if row_indices:
            return [
                ('Merge cluster', lambda df, log: merge_near_duplicates(df, log, col, row_indices)),
                ('Flag cluster', lambda df, log: flag_near_duplicates(df, log, col, row_indices)),
            ]

    if issue_type == 'pattern_mismatch' and col:
        pattern = issue.get('sample_data', {}).get(col, {}).get('pattern')
        if pattern:
            return [
                ('Flag invalid', lambda df, log: flag_invalid_patterns(df, log, col, pattern)),
                ('Drop invalid rows', lambda df, log: drop_invalid_rows(df, log, col, pattern)),
            ]

    if issue_type == 'out_of_range' and col:
        sample = issue.get('sample_data', {}).get(col, {})
        lo, hi = sample.get('valid_lo'), sample.get('valid_hi')
        if lo is not None or hi is not None:
            return [
                ('Clip to range', lambda df, log: drop_out_of_range_rows(df, log, col, lo, hi)),
                ('Drop invalid rows', lambda df, log: drop_out_of_range_rows(df, log, col, lo, hi)),
            ]

    return []


def _apply_action(idx: int, handler) -> None:
    st.session_state['df'] = handler(st.session_state['df'], st.session_state['cleaning_log'])
    _dismiss_issue(idx)


def _dismiss_issue(idx: int) -> None:
    st.session_state['issues'].pop(idx)
    st.rerun()


def _humanize(s: str) -> str:
    return s.replace('_', ' ').title()

# ---------------------------------------------------------------------------
# Stage: done
# ---------------------------------------------------------------------------

def render_done() -> None:
    st.header('Cleaning Complete')
    df = st.session_state['df']
    original_df = st.session_state['original_df']
    cleaning_log = st.session_state['cleaning_log']

    col1, col2, col3 = st.columns(3)
    col1.metric('Original Rows', f'{len(original_df):,}')
    col2.metric('Cleaned Rows', f'{len(df):,}')
    col3.metric('Transforms Applied', len(cleaning_log))

    st.divider()
    st.subheader('Cleaned Data Preview')
    st.dataframe(df.head(20), use_container_width=True)

    st.divider()
    st.subheader('Downloads')
    csv_bytes = df.to_csv(index=False).encode('utf-8')
    log_bytes = json.dumps(cleaning_log, indent=2, default=str).encode('utf-8')

    col1, col2 = st.columns(2)
    col1.download_button('Download cleaned CSV', csv_bytes, 'cleaned_data.csv', 'text/csv')
    col2.download_button('Download cleaning log (JSON)', log_bytes, 'cleaning_log.json', 'application/json')

    if cleaning_log:
        st.subheader('Cleaning log')
        st.json(cleaning_log)

    st.divider()
    if st.button('Start Over', key='restart_btn'):
        _reset_to_upload()
        st.rerun()


def _reset_to_upload() -> None:
    st.session_state['df'] = None
    st.session_state['original_df'] = None
    st.session_state['issues'] = []
    st.session_state['cleaning_log'] = []
    st.session_state['stage'] = 'upload'

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

STAGE_RENDERERS = {
    'upload': render_upload,
    'diagnose': render_diagnose,
    'decide': render_decide,
    'done': render_done,
}

st.title('Neatly — AI Data Cleaning Copilot')
STAGE_RENDERERS[st.session_state['stage']]()
