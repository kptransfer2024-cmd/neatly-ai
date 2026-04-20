"""Helpers for initialising and accessing st.session_state keys."""
import pandas as pd
import streamlit as st


def init_state() -> None:
    """Initialize all required session_state keys if they don't exist."""
    INITIAL_STATE = {
        'df': None,
        'original_df': None,
        'issues': [],
        'cleaning_log': [],
        'stage': 'upload',
        'df_history': [],         # list of snapshot dicts for the Changes tab
        '_preview_idx': None,     # index of the issue currently being previewed
        '_preview_handler': None, # callable to run for the preview diff
        '_preview_label': None,   # label of the previewed action
    }
    for key, value in INITIAL_STATE.items():
        if key not in st.session_state:
            st.session_state[key] = value


def get_df() -> pd.DataFrame | None:
    """Get the current working DataFrame from session_state."""
    return st.session_state.get('df')


def set_stage(stage: str) -> None:
    """Set the current stage in session_state."""
    st.session_state['stage'] = stage
