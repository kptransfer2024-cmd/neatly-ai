"""Streamlit entry point.

Manages stage transitions and renders the appropriate view for each stage.
Reads and writes st.session_state only — no business logic here.
"""
import streamlit as st
import pandas as pd

from orchestrator import run_diagnosis
from transformation_executor import (
    drop_duplicates,
    fill_missing,
    drop_missing,
    cast_column,
    normalize_text,
    clip_outliers,
)

# ---------------------------------------------------------------------------
# Session state initialisation
# ---------------------------------------------------------------------------

INITIAL_STATE = {
    'df': None,
    'original_df': None,
    'issues': [],
    'cleaning_log': [],
    'stage': 'upload',
}

for key, value in INITIAL_STATE.items():
    if key not in st.session_state:
        st.session_state[key] = value

# ---------------------------------------------------------------------------
# Stage: upload
# ---------------------------------------------------------------------------

def render_upload() -> None:
    raise NotImplementedError

# ---------------------------------------------------------------------------
# Stage: diagnose
# ---------------------------------------------------------------------------

def render_diagnose() -> None:
    raise NotImplementedError

# ---------------------------------------------------------------------------
# Stage: decide
# ---------------------------------------------------------------------------

def render_decide() -> None:
    raise NotImplementedError

# ---------------------------------------------------------------------------
# Stage: done
# ---------------------------------------------------------------------------

def render_done() -> None:
    raise NotImplementedError

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
