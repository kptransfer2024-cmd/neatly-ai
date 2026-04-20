"""Streamlit entry point.

Manages stage transitions and renders the appropriate view for each stage.
Reads and writes st.session_state only — no business logic here.
"""
import json

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

st.set_page_config(
    page_title="Neatly — AI Data Cleaning",
    page_icon="✦",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
:root {
  --accent: #3b82f6;
  --success: #34d399;
  --danger: #f87171;
  --warning: #fbbf24;
  --muted: #64748b;
  --border: #1e2d40;
  --border-strong: #2a3f58;
}

.block-container { max-width: 920px; padding: 1.5rem 1rem; }
h1, h2, h3, h4, h5, h6 { font-weight: 600; line-height: 1.3; }

.stepper {
  display: flex; align-items: center; justify-content: center;
  gap: 0.75rem; margin-bottom: 1.75rem;
}
.step {
  display: flex; flex-direction: column; align-items: center;
  color: var(--muted); font-size: 13px;
}
.step-circle {
  width: 36px; height: 36px; border-radius: 50%;
  border: 1px solid currentColor; opacity: 0.35;
  display: flex; align-items: center; justify-content: center;
  font-weight: 600; margin-bottom: 0.4rem;
}
.step.active .step-circle {
  background: var(--accent); border-color: var(--accent);
  color: white; opacity: 1;
}
.step.done .step-circle {
  border-color: var(--success); color: var(--success); opacity: 1;
}
.step-arrow { color: var(--muted); font-size: 14px; margin-top: 0.65rem; opacity: 0.25; }
.step-arrow:last-child { display: none; }

.neat-card {
  border-radius: 6px; padding: 1rem 1.125rem; margin-bottom: 0.75rem;
  border: 1px solid var(--border);
}
.neat-card.sev-high   { border-left: 3px solid var(--danger); }
.neat-card.sev-medium { border-left: 3px solid var(--warning); }
.neat-card.sev-low    { border-left: 3px solid var(--success); }

.sev-badge {
  display: inline-block; padding: 1px 7px; border-radius: 3px;
  font-size: 11px; font-weight: 600; letter-spacing: 0.02em;
}
.sev-high   .sev-badge { background: rgba(248,113,113,0.12); color: var(--danger); }
.sev-medium .sev-badge { background: rgba(251,191,36,0.12);  color: var(--warning); }
.sev-low    .sev-badge { background: rgba(52,211,153,0.12);  color: var(--success); }

/* Buttons always fill their column and stay consistent height */
.stButton { width: 100%; }
.stButton > button {
  width: 100%; border-radius: 5px; font-weight: 500; font-size: 13px;
  transition: opacity 0.15s; border: 1px solid var(--border-strong) !important;
  padding: 0.35rem 0.75rem;
}
button[kind="primary"] {
  background: var(--accent) !important; color: white !important;
  border-color: var(--accent) !important;
}
button[kind="primary"]:hover { opacity: 0.88; }
button.neatly-preview {
  border-color: #38bdf8 !important; color: #38bdf8 !important;
  background: transparent !important;
}
button.neatly-skip {
  border-color: #2a3a50 !important; color: var(--muted) !important;
  background: transparent !important;
}
button.neatly-undo {
  border-color: #2a3a50 !important; color: #94a3b8 !important;
  background: transparent !important; font-size: 12px !important;
}

/* Metric containers */
[data-testid="metric-container"] {
  border-radius: 6px; padding: 1rem;
  border: 1px solid var(--border);
}

/* Tighter spacing inside issue cards */
.neat-card .stMarkdown p { margin: 0.15rem 0; }
.neat-card .stMarkdown    { margin-bottom: 0.25rem; }
.neat-card .stCaption     { margin-top: 0.1rem; margin-bottom: 0.1rem; }
.neat-card .stExpander    { margin-top: 0.35rem; }

/* Tab content: align to top consistently */
[data-baseweb="tab-panel"] { padding-top: 0.75rem !important; }

/* Download buttons */
.stDownloadButton { width: 100%; }
.stDownloadButton > button { width: 100%; border-radius: 5px; font-weight: 500; }

[role="tablist"] button { font-weight: 500; }
[data-testid="stAppViewContainer"] { padding-top: 0; }

/* Hide the redundant st.title() h1 — each stage has its own header */
[data-testid="stAppViewContainer"] > .main > .block-container > div:first-child h1 { display: none; }

footer { display: none; }
</style>
<script>
(function() {
    function tag() {
        document.querySelectorAll('.stButton > button').forEach(function(btn) {
            var t = (btn.querySelector('p') || btn).textContent.trim();
            if (t === 'View') btn.classList.add('neatly-preview');
            else if (t === 'Skip') btn.classList.add('neatly-skip');
            else if (t.indexOf('Undo') !== -1) btn.classList.add('neatly-undo');
        });
    }
    var obs = new MutationObserver(tag);
    obs.observe(document.body, {childList: true, subtree: true});
    tag();
})();
</script>
""", unsafe_allow_html=True)

from orchestrator import run_diagnosis
from transformation_executor import (
    cast_column,
    clip_outliers,
    coerce_to_numeric,
    drop_column,
    drop_duplicates,
    drop_invalid_rows,
    drop_missing,
    drop_non_numeric_rows,
    drop_out_of_range_dates,
    drop_out_of_range_rows,
    drop_whitespace_rows,
    fill_missing,
    clip_to_range,
    flag_all_near_duplicates,
    flag_invalid_patterns,
    flag_near_duplicates,
    mask_pii,
    merge_all_near_duplicates,
    merge_near_duplicates,
    normalize_text,
    null_out_whitespace,
    standardize_phone,
    standardize_dates,
    standardize_currency,
    find_replace,
    fill_with_constant,
    apply_custom_regex,
)
from utils import code_snippets
from utils.diff_engine import compute_diff, render_diff
from utils.file_ingestion import parse_uploaded_file
from utils.db_ingestion import (
    build_connection_string,
    list_tables,
    load_table,
    load_query,
    create_connection,
    write_table,
)
from streamlit_ace import st_ace
from utils.analytics import init_session, log_event
from utils.context_summary import summarize_data_context
from utils.session_state import init_state

load_dotenv()

_HISTORY_CAP = 20
_UNDO_CAP = 10


# ---------------------------------------------------------------------------
# Cached DB helpers — engine and DataFrames survive across Streamlit reruns
# ---------------------------------------------------------------------------

@st.cache_resource
def _get_db_engine(conn_str: str):
    return create_connection(conn_str)


@st.cache_data(ttl=600)
def _load_table_cached(conn_str: str, table_name: str, limit: int) -> pd.DataFrame:
    engine = _get_db_engine(conn_str)
    return pd.read_sql(f"SELECT * FROM {table_name} LIMIT {limit}", engine)


@st.cache_data(ttl=600)
def _load_query_cached(conn_str: str, sql_query: str, limit: int) -> pd.DataFrame:
    engine = _get_db_engine(conn_str)
    query = f"SELECT * FROM ({sql_query.strip()}) AS _q LIMIT {limit}"
    return pd.read_sql(query, engine)


# ---------------------------------------------------------------------------
# Session state initialisation
# ---------------------------------------------------------------------------

init_state()
init_session()   # generates session_id and fires session_started once per browser session

# ---------------------------------------------------------------------------
# Stage: upload
# ---------------------------------------------------------------------------

def _render_stage_bar(current: str) -> None:
    """Render numbered progress stepper across all stages."""
    stages = ['upload', 'diagnose', 'decide', 'done']
    labels = ['Upload', 'Diagnose', 'Review', 'Done']
    current_idx = stages.index(current)

    steps_html = '<div class="stepper">'
    for i, (stage, label) in enumerate(zip(stages, labels)):
        step_class = 'active' if stage == current else ('done' if i < current_idx else '')
        symbol = i + 1
        steps_html += f'''
        <div class="step {step_class}">
            <div class="step-circle">{symbol if i >= current_idx else '✓'}</div>
            <div style="font-size: 12px; font-weight: 500;">{label}</div>
        </div>
        '''
        if i < len(stages) - 1:
            steps_html += '<div class="step-arrow">→</div>'

    steps_html += '</div>'
    st.markdown(steps_html, unsafe_allow_html=True)


def render_upload() -> None:
    _render_stage_bar('upload')

    # Hero section
    st.markdown(
        """
        <div style="text-align: center; margin: 2rem 0 3rem 0;">
            <div style="font-size: 3rem; margin-bottom: 0.5rem;">✦</div>
            <h1 style="margin: 0; font-size: 2.5rem; color: #f4f4f5;">Neatly</h1>
            <p style="margin: 0.5rem 0 0 0; font-size: 1.1rem; color: #71717a;">Upload data. Get results. No headaches.</p>
        </div>
        """,
        unsafe_allow_html=True
    )

    tab_file, tab_db = st.tabs(['📁 File Upload', '🗄️ Database'])

    with tab_file:
        _render_file_upload()

    with tab_db:
        _render_database_loader()


def _render_context_summary(df: pd.DataFrame, source_name: str | None = None) -> None:
    """Render the brief data-context intro above the preview table."""
    summary = summarize_data_context(df, source_name=source_name)
    if summary:
        st.info(summary)


def _render_file_upload() -> None:
    uploaded_file = st.file_uploader(
        'Choose a file (CSV, TSV, JSON, Excel, Parquet)',
        type=['csv', 'tsv', 'json', 'xlsx', 'xls', 'parquet'],
        key='file_uploader',
    )

    if not uploaded_file:
        return

    # Cache the parsed DataFrame by (name, size) so re-renders don't re-parse the file.
    _cache_key = (uploaded_file.name, uploaded_file.size)
    if st.session_state.get('_upload_cache_key') != _cache_key:
        try:
            df = parse_uploaded_file(uploaded_file)
        except Exception as e:
            st.error(f'Error reading file: {e}')
            return
        st.session_state['_upload_cache_key'] = _cache_key
        st.session_state['_upload_cached_df'] = df
    else:
        df = st.session_state['_upload_cached_df']

    _MAX_ROWS, _MAX_COLS = 2_000_000, 500
    if len(df) > _MAX_ROWS:
        st.error(f'File has {len(df):,} rows — limit is {_MAX_ROWS:,}. Upload a smaller sample.')
        return
    if len(df.columns) > _MAX_COLS:
        st.error(f'File has {len(df.columns)} columns — limit is {_MAX_COLS}.')
        return

    st.success(f'Loaded {len(df):,} rows, {len(df.columns)} columns')
    _render_context_summary(df, source_name=uploaded_file.name)
    st.dataframe(df.head(10), use_container_width=True)

    if st.button('Start Diagnosis', key='diagnose_file_btn', type='primary'):
        log_event('file_uploaded', rows=len(df), columns=len(df.columns), source='file')
        st.session_state['original_df'] = df.copy()
        st.session_state['df'] = df
        st.session_state['issues'] = []
        st.session_state['cleaning_log'] = []
        st.session_state['df_history'] = []
        st.session_state['_undo_stack'] = []
        st.session_state['_custom_rules'] = []
        st.session_state['_input_source'] = 'file'
        st.session_state.pop('_db_source_config', None)
        st.session_state.pop('_db_source_table', None)
        st.session_state['stage'] = 'diagnose'
        _clear_preview()
        st.rerun()


def _render_database_loader() -> None:
    """Render the database connection UI.

    Connection state is persisted in session_state so the table selector
    remains visible across reruns after a successful connect.
    """
    st.subheader('Connect to Database')

    col1, _ = st.columns(2)
    with col1:
        db_type = st.selectbox(
            'Database Type',
            ['PostgreSQL', 'MySQL', 'MySQL Workbench (Local)', 'SQLite', 'SQL Server'],
            key='db_type_select',
        )

    # Reset stored connection when db_type changes
    if st.session_state.get('_db_connected_type') != db_type:
        st.session_state.pop('_db_conn_str', None)
        st.session_state.pop('_db_tables', None)
        st.session_state.pop('_db_connected_type', None)
        st.session_state.pop('_db_loaded_df', None)

    # ---- type-specific help ----
    if db_type == 'MySQL Workbench (Local)':
        st.info(
            '**MySQL Workbench (Local)**  \n'
            'Use the same credentials as your Workbench connection:  \n'
            '1. Open MySQL Workbench → right-click your connection → **Edit Connection**  \n'
            '2. Copy **Hostname**, **Port**, **Username**, and **Default Schema** (= Database)  \n'
            '3. Use the password you set when installing MySQL Server.'
        )
    elif db_type == 'MySQL':
        st.info(
            '**MySQL (remote)**  \n'
            'Connect to any remote MySQL server. For a local MySQL instance use '
            '**MySQL Workbench (Local)** instead.'
        )
    elif db_type == 'PostgreSQL':
        st.info(
            '**PostgreSQL**  \n'
            'Default port is **5432**. For cloud databases (Supabase, Neon, RDS) '
            'paste the full host string from the provider dashboard.'
        )
    elif db_type == 'SQL Server':
        st.info(
            '**SQL Server / Azure SQL**  \n'
            'Requires ODBC Driver 17 for SQL Server installed on this machine. '
            'Default port is **1433**.'
        )

    # Internal dialect type for build_connection_string (Workbench == MySQL)
    _dialect = 'MySQL' if db_type == 'MySQL Workbench (Local)' else db_type

    # ---- connection form ----
    if db_type == 'SQLite':
        db_path = st.text_input(
            'Database File Path',
            placeholder='C:/path/to/database.db',
            key='sqlite_path',
        )
        if st.button('Connect', key='connect_sqlite_btn'):
            try:
                conn_str = build_connection_string('SQLite', path=db_path)
                engine = _get_db_engine(conn_str)
                tables = list_tables(engine)
                if not tables:
                    st.warning('No tables found in this database.')
                else:
                    st.session_state['_db_conn_str'] = conn_str
                    st.session_state['_db_tables'] = tables
                    st.session_state['_db_connected_type'] = db_type
                    st.session_state['_db_source_config'] = {'db_type': 'SQLite', 'path': db_path}
                    st.rerun()
            except Exception as e:
                st.error(f'Connection failed: {e}')
    else:
        default_host = 'localhost' if db_type == 'MySQL Workbench (Local)' else ''
        default_user = 'root' if db_type == 'MySQL Workbench (Local)' else ''

        col1, col2 = st.columns(2)
        with col1:
            host = st.text_input('Host', value=default_host, key=f'{db_type}_host')
            port = st.number_input('Port', value=_get_default_port(db_type), key=f'{db_type}_port')
        with col2:
            database = st.text_input('Database / Schema', placeholder='my_database', key=f'{db_type}_database')

        col1, col2 = st.columns(2)
        with col1:
            user = st.text_input('Username', value=default_user, key=f'{db_type}_user')
        with col2:
            password = st.text_input('Password', type='password', key=f'{db_type}_password')

        if st.button('Connect', key=f'connect_{db_type}_btn'):
            if not host or not database or not user:
                st.warning('Please fill in Host, Database, and Username.')
            else:
                try:
                    conn_str = build_connection_string(
                        _dialect,
                        host=host,
                        port=int(port),
                        database=database,
                        user=user,
                        password=password,
                    )
                    engine = _get_db_engine(conn_str)
                    tables = list_tables(engine)
                    if not tables:
                        st.warning('Connected but no tables found in this database/schema.')
                    else:
                        st.session_state['_db_conn_str'] = conn_str
                        st.session_state['_db_tables'] = tables
                        st.session_state['_db_connected_type'] = db_type
                        st.session_state['_db_source_config'] = {
                            'db_type': db_type,
                            'host': host, 'port': int(port),
                            'user': user, 'password': password,
                            'database': database,
                        }
                        st.rerun()
                except Exception as e:
                    st.error(f'Connection failed: {e}')
                    if db_type == 'MySQL Workbench (Local)':
                        st.caption(
                            'Tip: Make sure MySQL Server is running. '
                            'Check MySQL Workbench → Server → Startup/Shutdown.'
                        )

    # ---- post-connect: table/query picker (persists across reruns) ----
    conn_str = st.session_state.get('_db_conn_str')
    tables = st.session_state.get('_db_tables')
    if not conn_str or not tables:
        return

    st.success(f'Connected — {len(tables)} table(s) found')
    row_limit = st.slider('Row Limit', 1_000, 2_000_000, 1_000_000, step=10_000, key='db_row_limit')
    load_mode = st.radio('Load Mode', ['Select Table', 'Custom Query'], horizontal=True, key='db_load_mode')

    if load_mode == 'Select Table':
        table_name = st.selectbox('Select Table', tables, key='db_table_select')
        if st.button('Load Table', key='db_load_table_btn', type='primary'):
            try:
                df = _load_table_cached(conn_str, table_name, row_limit)
                st.session_state['_db_loaded_df'] = df
                st.session_state['_db_source_table'] = table_name
                st.rerun()
            except Exception as e:
                st.error(f'Load failed: {e}')
    else:
        sql_query = st.text_area('SQL Query', placeholder='SELECT * FROM my_table', height=100, key='db_sql_query')
        if st.button('Run Query', key='db_run_query_btn', type='primary'):
            if not sql_query.strip():
                st.info('Enter a SQL SELECT query above.')
            else:
                try:
                    df = _load_query_cached(conn_str, sql_query, row_limit)
                    st.session_state['_db_loaded_df'] = df
                    st.rerun()
                except Exception as e:
                    st.error(f'Query failed: {e}')

    loaded_df = st.session_state.get('_db_loaded_df')
    if loaded_df is not None:
        _finalize_database_load(loaded_df)


def _finalize_database_load(df: pd.DataFrame) -> None:
    """Show preview and start diagnosis for database-loaded data."""
    st.success(f'Loaded {len(df):,} rows, {len(df.columns)} columns')
    table_name = st.session_state.get('_db_source_table')
    _render_context_summary(df, source_name=table_name)
    st.dataframe(df.head(10), use_container_width=True)

    if st.button('Start Diagnosis', key='diagnose_db_btn', type='primary'):
        log_event('file_uploaded', rows=len(df), columns=len(df.columns), source='database')
        st.session_state['original_df'] = df.copy()
        st.session_state['df'] = df
        st.session_state['issues'] = []
        st.session_state['cleaning_log'] = []
        st.session_state['df_history'] = []
        st.session_state['_undo_stack'] = []
        st.session_state['_custom_rules'] = []
        st.session_state['_input_source'] = 'database'
        st.session_state.pop('_db_loaded_df', None)
        st.session_state['stage'] = 'diagnose'
        _clear_preview()
        st.rerun()


def _get_default_port(db_type: str) -> int:
    """Get the default port for a database type."""
    ports = {'PostgreSQL': 5432, 'MySQL': 3306, 'MySQL Workbench (Local)': 3306, 'SQL Server': 1433}
    return ports.get(db_type, 5432)

# ---------------------------------------------------------------------------
# Stage: diagnose
# ---------------------------------------------------------------------------

def render_diagnose() -> None:
    st.markdown(
        """
        <div style="text-align: center; margin: 4rem 0;">
            <div style="font-size: 2rem; margin-bottom: 1rem;">✓</div>
            <p style="font-size: 1.1rem; color: #71717a;">Analyzing your data…</p>
        </div>
        """,
        unsafe_allow_html=True
    )
    with st.spinner('Running detectors and generating explanations…'):
        try:
            result = run_diagnosis(st.session_state['df'])
        except Exception as exc:
            st.session_state['stage'] = 'upload'
            st.error(f'Diagnosis failed: {exc}. Please try re-uploading your file.')
            st.rerun()
            return

        issues = result['issues']
        st.session_state['issues'] = issues
        st.session_state['column_contexts'] = result.get('column_contexts', [])
        st.session_state['stage'] = 'decide'

        failed = result.get('failed_detectors', [])
        if failed:
            st.session_state['_failed_detectors'] = failed

        log_event(
            'diagnosis_completed',
            n_issues=len(issues),
            issue_types=list({i.get('type') for i in issues if i.get('type')}),
        )
    st.rerun()

# ---------------------------------------------------------------------------
# Stage: decide
# ---------------------------------------------------------------------------

_ISSUE_CATEGORIES = {
    'missing_value': 'Completeness',
    'whitespace_values': 'Completeness',
    'constant_column': 'Completeness',
    'type_mismatch': 'Accuracy',
    'pattern_mismatch': 'Accuracy',
    'out_of_range': 'Accuracy',
    'mixed_type': 'Accuracy',
    'duplicates': 'Consistency',
    'near_duplicates': 'Consistency',
    'duplicate_column': 'Consistency',
    'inconsistent_format': 'Consistency',
    'outliers': 'Validity',
    'id_column': 'Validity',
    'pii_detected': 'Validity',
}

_CATEGORY_ICONS = {
    'Completeness': '🔴',
    'Accuracy': '🟡',
    'Consistency': '🟠',
    'Validity': '🔵',
}

def _group_issues_by_category(issues: list[dict]) -> dict[str, list[tuple[int, dict]]]:
    """Group issues by category, preserving original list index.

    Returns dict mapping category name → list of (original_idx, issue_dict) tuples.
    This preserves the ability to dismiss issues by original index.
    """
    grouped = {cat: [] for cat in _ISSUE_CATEGORIES.values()}
    for idx, issue in enumerate(issues):
        issue_type = issue.get('type', 'outliers')
        category = _ISSUE_CATEGORIES.get(issue_type, 'Validity')
        grouped[category].append((idx, issue))
    return grouped

_STATS_HIDE_KEYS = {
    'type', 'column', 'columns', 'sub_type', 'explanation', 'summary',
    'sample_values', 'example_values', 'sample_indices', 'row_indices',
    'dtype', 'detector', 'severity', 'sample_data', 'actions',
}


def render_decide() -> None:
    _render_stage_bar('decide')

    # Toast feedback from last applied action
    feedback = st.session_state.pop('_last_action_feedback', None)
    if feedback:
        st.toast(feedback)

    # Undo button — top right, only when stack has entries
    undo_stack = st.session_state.get('_undo_stack', [])
    if undo_stack:
        _, undo_col = st.columns([8, 2])
        if undo_col.button(f'↩ Undo: {undo_stack[-1]["label"]}', key='undo_btn', use_container_width=True):
            _undo_last_action()

    st.header('Review & Fix Issues')

    # Live dataset window — always visible, updates after each action
    _render_live_data_window()

    issues = st.session_state['issues']
    history = st.session_state['df_history']

    failed = st.session_state.pop('_failed_detectors', None)
    if failed:
        st.warning(
            f"⚠️ {len(failed)} detector(s) could not run and were skipped: "
            f"`{'`, `'.join(failed)}`. Results may be incomplete."
        )

    changes_label = f"📋 Changes ({len(history)})" if history else "📋 Changes"
    custom_rules = st.session_state.get('_custom_rules', [])
    rules_label = f"✏️ Custom Rules ({len(custom_rules)})" if custom_rules else "✏️ Custom Rules"
    tab_issues, tab_changes, tab_code, tab_rules = st.tabs(
        ['🔍 Issues', changes_label, '🧪 Custom Code', rules_label]
    )

    with tab_issues:
        _render_column_context_panel()
        if not issues:
            st.info('No issues found — your data looks clean.')
            if st.button('Finish', key='finish_btn', type='primary'):
                log_event("session_completed", n_actions=len(st.session_state["cleaning_log"]), issues_remaining=0)
                st.session_state["stage"] = "done"
                st.rerun()
        else:
            grouped = _group_issues_by_category(issues)

            # Build tab names with issue counts
            tab_names = []
            for cat in ['Completeness', 'Accuracy', 'Consistency', 'Validity']:
                count = len(grouped[cat])
                icon = _CATEGORY_ICONS[cat]
                if count > 0:
                    tab_names.append(f"{icon} {cat} ({count})")
                else:
                    tab_names.append(f"{icon} {cat}")

            category_tabs = st.tabs(tab_names)

            for tab, category in zip(category_tabs, ['Completeness', 'Accuracy', 'Consistency', 'Validity']):
                with tab:
                    issues_in_cat = grouped[category]
                    if not issues_in_cat:
                        st.info(f"✓ No {category.lower()} issues detected.")
                    else:
                        st.caption(f"{len(issues_in_cat)} issue(s) in this category")
                        for orig_idx, issue in issues_in_cat:
                            _render_issue_card(orig_idx, issue)

            st.divider()
            col1, col2 = st.columns([3, 1])
            if col1.button('Done Reviewing', key='done_review_btn', type='primary', use_container_width=True):
                log_event("session_completed", n_actions=len(st.session_state["cleaning_log"]), issues_remaining=len(issues))
                st.session_state["stage"] = "done"
                st.rerun()
            if col2.button('Start Over', key='restart_decide_btn', use_container_width=True):
                _reset_to_upload()
                st.rerun()

    with tab_changes:
        render_changes_tab()

    with tab_code:
        render_custom_code_tab()

    with tab_rules:
        render_custom_rules_tab()


def _render_near_duplicate_quick_actions(issues: list) -> None:
    """If ≥2 near-duplicate clusters are present, offer one-click bulk decisions.

    Each cluster gets its own card, so a CSV with 10 clusters takes 10 clicks.
    This banner lets a user resolve every near-duplicate cluster at once.
    """
    nd_indices = [i for i, iss in enumerate(issues) if iss.get('type') == 'near_duplicates']
    if len(nd_indices) < 2:
        return

    clusters = [
        {
            'column': (issues[i].get('columns') or [issues[i].get('column')])[0],
            'row_indices': issues[i].get('row_indices') or [],
        }
        for i in nd_indices
    ]

    with st.container(border=True):
        st.markdown(f"**Quick actions — {len(nd_indices)} near-duplicate clusters detected**")
        st.caption('Apply one decision across every cluster below.')
        c1, c2, c3 = st.columns(3)
        if c1.button('Merge all clusters', key='nd_bulk_merge', type='primary'):
            st.session_state['df'] = merge_all_near_duplicates(
                st.session_state['df'], st.session_state['cleaning_log'], clusters,
            )
            _dismiss_issues_by_type('near_duplicates')
        if c2.button('Flag all clusters', key='nd_bulk_flag'):
            st.session_state['df'] = flag_all_near_duplicates(
                st.session_state['df'], st.session_state['cleaning_log'], clusters,
            )
            _dismiss_issues_by_type('near_duplicates')
        if c3.button('Skip all', key='nd_bulk_skip'):
            _dismiss_issues_by_type('near_duplicates')


def _dismiss_issues_by_type(issue_type: str) -> None:
    """Remove every issue of *issue_type* from session_state and rerun."""
    st.session_state['issues'] = [
        iss for iss in st.session_state['issues'] if iss.get('type') != issue_type
    ]
    st.rerun()


def _render_issue_card(idx: int, issue: dict) -> None:
    issue_type = issue.get('type', 'issue')
    columns = issue.get('columns') or ([issue['column']] if issue.get('column') else [])
    column = columns[0] if columns else None
    title = _humanize(issue_type) + (f" — `{', '.join(columns)}`" if columns else "")
    sev = issue.get('severity', 'low')

    # Start the card div with severity class for left-border color
    st.markdown(f'<div class="neat-card sev-{sev}">', unsafe_allow_html=True)

    # Title with severity badge
    sev_badge = f'<span class="sev-badge">{sev}</span>'
    st.markdown(f"**{title}** &nbsp; {sev_badge}", unsafe_allow_html=True)

    # Explanation
    explanation = issue.get('explanation') or issue.get('summary')
    if explanation:
        st.write(explanation)

    # Stats
    stats = {k: v for k, v in issue.items() if k not in _STATS_HIDE_KEYS}
    if stats:
        st.caption(" • ".join(f"{k}: {v}" for k, v in stats.items()))

    # Detection snippet
    snippet = code_snippets.DETECTION_SNIPPETS.get(issue_type)
    if snippet:
        with st.expander('🔍 How was this detected?'):
            formatted_snippet = snippet.format(col=column) if column else snippet
            st.code(formatted_snippet, language='python')

    # Actions
    actions = _actions_for(issue)
    if not actions:
        st.caption("_No automatic fix available for this issue._")
    else:
        # Layout: action buttons... | View | Skip
        cols = st.columns(len(actions) + 2)
        for btn_col, (lbl, handler) in zip(cols[:-2], actions):
            if btn_col.button(lbl, key=f'act_{idx}_{lbl}', type='primary'):
                _apply_action(idx, handler, lbl)
        if cols[-2].button('View', key=f'prev_{idx}'):
            first_label, first_handler = actions[0]
            st.session_state['_preview_idx'] = idx
            st.session_state['_preview_handler'] = first_handler
            st.session_state['_preview_label'] = first_label
            st.rerun()
        if cols[-1].button('Skip', key=f'skip_{idx}'):
            _clear_preview()
            issue = st.session_state['issues'][idx] if idx < len(st.session_state['issues']) else {}
            log_event('issue_skipped', issue_type=issue.get('type'), column=(issue.get('columns') or [None])[0])
            _dismiss_issue(idx)

    # End card div
    st.markdown('</div>', unsafe_allow_html=True)

    # Preview panel (outside the card)
    if st.session_state.get('_preview_idx') == idx:
        _render_preview_panel(idx)


def _render_preview_panel(idx: int) -> None:
    handler = st.session_state['_preview_handler']
    label = st.session_state['_preview_label']
    try:
        df_current = st.session_state['df']
        df_preview = handler(df_current.copy(), [])
    except Exception as e:
        st.error(f'Preview failed: {e}')
        return

    diff = compute_diff(df_current, df_preview)
    cols_affected = diff.get('columns_affected', [])

    with st.container(border=True):
        st.markdown(f"**If you apply: {label}**")

        parts = []
        if diff['rows_changed']:
            parts.append(f"{diff['rows_changed']} rows updated")
        if diff['rows_removed']:
            parts.append(f"{diff['rows_removed']} rows removed")
        if diff['rows_added']:
            parts.append(f"{diff['rows_added']} rows added")
        if diff['cells_changed'] and not parts:
            parts.append(f"{diff['cells_changed']} cells changed")
        st.caption(" • ".join(parts) if parts else "No row-level changes — dtype or flag column will be updated")

        display_df = df_preview.head(30)
        valid_cols = [c for c in cols_affected if c in display_df.columns]
        if valid_cols:
            styled = display_df.style.set_properties(
                subset=valid_cols,
                **{'background-color': 'rgba(52, 211, 153, 0.18)'},
            )
            st.dataframe(styled, use_container_width=True, height=220)
            st.caption(f"Green = changed columns: {', '.join(f'`{c}`' for c in valid_cols)}")
        elif diff['rows_removed'] and df_preview.empty:
            st.info("All rows would be removed.")
        else:
            st.dataframe(display_df, use_container_width=True, height=220)

        if diff['rows_removed'] and not df_preview.empty:
            st.caption(f"⚠️ {diff['rows_removed']} rows will be removed (not shown above)")

        if st.button('Close', key=f'prev_close_{idx}'):
            _clear_preview()
            st.rerun()


def _render_live_data_window() -> None:
    """Always-visible current dataset pane with last-changed columns highlighted in blue."""
    df = st.session_state.get('df')
    if df is None:
        return
    highlight_cols = st.session_state.get('_highlight_cols', [])
    last_label = st.session_state.get('_last_action_label', '')

    title = f"📊 Current Dataset — {len(df):,} rows × {len(df.columns)} cols"
    if last_label:
        title += f"  ·  last: {last_label}"

    with st.expander(title, expanded=True):
        display_df = df.head(30)
        valid_cols = [c for c in highlight_cols if c in display_df.columns]
        if valid_cols:
            st.caption(f"Recently changed columns (blue): {', '.join(f'`{c}`' for c in valid_cols)}")
            styled = display_df.style.set_properties(
                subset=valid_cols,
                **{'background-color': 'rgba(59, 130, 246, 0.15)'},
            )
            st.dataframe(styled, use_container_width=True, height=230)
        else:
            st.dataframe(display_df, use_container_width=True, height=230)
        if len(df) > 30:
            st.caption(f"Showing first 30 of {len(df):,} rows.")


def _clear_preview() -> None:
    st.session_state['_preview_idx'] = None
    st.session_state['_preview_handler'] = None
    st.session_state['_preview_label'] = None


_HEALTH_ICON = {'good': '🟢', 'warn': '🟡', 'bad': '🔴'}
_ROLE_LABEL = {
    'id': 'ID', 'metric': 'Metric', 'category': 'Category',
    'datetime': 'Date/Time', 'flag': 'Flag', 'contact': 'Contact', 'text': 'Text',
}


def _render_column_context_panel() -> None:
    contexts = st.session_state.get('column_contexts', [])
    if not contexts:
        return
    good = sum(1 for c in contexts if c['health'] == 'good')
    warn = sum(1 for c in contexts if c['health'] == 'warn')
    bad = sum(1 for c in contexts if c['health'] == 'bad')
    with st.expander(f"📊 Column Profiles ({len(contexts)} columns)", expanded=False):
        st.caption(f"🟢 {good} good  •  🟡 {warn} warning  •  🔴 {bad} needs attention")
        pairs = [contexts[i:i+2] for i in range(0, len(contexts), 2)]
        for pair in pairs:
            grid = st.columns(len(pair))
            for g_col, ctx in zip(grid, pair):
                _render_column_card(g_col, ctx)


def _render_column_card(col_widget, ctx: dict) -> None:
    icon = _HEALTH_ICON.get(ctx['health'], '⚪')
    role = _ROLE_LABEL.get(ctx['inferred_role'], ctx['inferred_role'].title())
    domain_tag = f"  `{ctx['domain']}`" if ctx['domain'] else ''
    stats = ctx.get('stats', {})
    null_str = f"{ctx['null_pct']:.1f}% null"
    with col_widget.container(border=True):
        st.markdown(f"**{icon} {ctx['column']}**")
        st.caption(f"{role}{domain_tag}  •  `{ctx['dtype']}`")
        if ctx['inferred_role'] == 'metric' and 'min' in stats:
            st.caption(f"{stats['min']} – {stats['max']}  •  mean {stats['mean']}  •  {null_str}")
        elif ctx['inferred_role'] == 'datetime' and 'min' in stats:
            st.caption(f"{stats['min']} → {stats['max']}  •  {null_str}")
        elif ctx['inferred_role'] == 'flag' and 'true_pct' in stats:
            st.caption(f"true: {stats['true_pct']:.1f}%  •  {null_str}")
        else:
            mode_part = f"  •  top: {stats['mode']!r}" if stats.get('mode') else ''
            st.caption(f"{ctx['cardinality']} unique{mode_part}  •  {null_str}")


def _actions_for(issue: dict) -> list[tuple[str, callable]]:
    """Map an issue to (button_label, handler(df, log) -> df) pairs."""
    issue_type = issue.get('type')
    cols = issue.get('columns') or ([issue['column']] if issue.get('column') else [])
    col = cols[0] if cols else None

    if issue_type == 'missing_value' and col:
        dtype = issue.get('dtype', '')
        actions: list[tuple[str, callable]] = []
        if dtype not in ('object', 'str', 'string'):
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

    if issue_type == 'pii_detected' and col:
        pii_type = issue.get('sample_data', {}).get(col, {}).get('pii_type', 'unknown')
        return [
            (f'Mask {pii_type}', lambda df, log: mask_pii(df, log, col, pii_type, 'partial')),
            ('Remove Column', lambda df, log: drop_column(df, col, log)),
        ]

    if issue_type == 'standardization_suggested' and col:
        std_type = issue.get('sample_data', {}).get(col, {}).get('standardization_type', 'unknown')
        if std_type == 'phone':
            return [('Standardize phone', lambda df, log: standardize_phone(df, log, col))]
        elif std_type == 'date':
            return [('Standardize dates', lambda df, log: standardize_dates(df, log, col))]
        elif std_type == 'currency':
            return [('Standardize currency', lambda df, log: standardize_currency(df, log, col))]

    if issue_type == 'out_of_range' and col:
        sample = issue.get('sample_data', {}).get(col, {})
        lo, hi = sample.get('valid_lo'), sample.get('valid_hi')
        if lo is not None or hi is not None:
            return [
                ('Clip to range', lambda df, log: clip_to_range(df, log, col, lo, hi)),
                ('Drop invalid rows', lambda df, log: drop_out_of_range_rows(df, log, col, lo, hi)),
            ]

    if issue_type == 'constant_column' and col:
        return [('Drop column', lambda df, log: drop_column(df, col, log))]

    if issue_type == 'id_column' and col:
        return [('Drop column', lambda df, log: drop_column(df, col, log))]

    if issue_type == 'date_out_of_range' and col:
        sample = issue.get('sample_data', {}).get(col, {})
        lo = sample.get('valid_lower')
        hi = sample.get('valid_upper')
        if lo and hi:
            return [('Drop out-of-range rows', lambda df, log: drop_out_of_range_dates(df, log, col, lo, hi))]

    if issue_type == 'whitespace_values' and col:
        return [
            ('Null out', lambda df, log: null_out_whitespace(df, log, col)),
            ('Drop rows', lambda df, log: drop_whitespace_rows(df, log, col)),
        ]

    if issue_type == 'mixed_type' and col:
        return [
            ('Coerce to numeric', lambda df, log: coerce_to_numeric(df, log, col)),
            ('Drop non-numeric rows', lambda df, log: drop_non_numeric_rows(df, log, col)),
        ]

    if issue_type == 'duplicate_column' and col:
        return [('Drop column', lambda df, log: drop_column(df, col, log))]

    return []


def _undo_last_action() -> None:
    stack = st.session_state.get('_undo_stack', [])
    if not stack:
        return
    entry = stack.pop()
    st.session_state['df'] = entry['df']
    st.session_state['issues'].insert(0, entry['issue'])
    if st.session_state['cleaning_log']:
        st.session_state['cleaning_log'].pop()
    if st.session_state['df_history']:
        st.session_state['df_history'].pop()
    st.rerun()


def _apply_action(idx: int, handler, label: str = 'Action') -> None:
    issue = st.session_state['issues'][idx] if idx < len(st.session_state['issues']) else {}
    df_before = st.session_state['df'].copy()
    try:
        st.session_state['df'] = handler(st.session_state['df'], st.session_state['cleaning_log'])
    except Exception as exc:
        st.session_state['df'] = df_before  # rollback — state is never half-mutated
        st.error(f'Could not apply "{label}": {exc}')
        return

    log = st.session_state['cleaning_log']
    log_entry = log[-1] if log else {}
    diff = compute_diff(df_before, st.session_state['df'])
    rows_affected = diff.get('rows_changed', 0) + diff.get('rows_removed', 0)
    log_event(
        'decision_made',
        action=label,
        issue_type=issue.get('type'),
        column=(issue.get('columns') or [None])[0],
        rows_affected=rows_affected,
    )

    # Store highlight + toast state for next render
    st.session_state['_highlight_cols'] = diff.get('columns_affected', [])
    st.session_state['_last_action_label'] = label
    parts = [f"**{label}** applied"]
    if diff.get('rows_changed'):
        parts.append(f"{diff['rows_changed']} rows updated")
    if diff.get('rows_removed'):
        parts.append(f"{diff['rows_removed']} rows removed")
    st.session_state['_last_action_feedback'] = ' — '.join(parts)

    history = st.session_state['df_history']
    history.append({
        'label': label,
        'log_entry': dict(log_entry),
        'df_before': df_before,
        'diff': diff,
        'applied_at': len(history),
    })
    if len(history) > _HISTORY_CAP:
        history.pop(0)

    undo_stack = st.session_state.setdefault('_undo_stack', [])
    undo_stack.append({'df': df_before, 'issue': issue, 'label': label})
    if len(undo_stack) > _UNDO_CAP:
        undo_stack.pop(0)

    _clear_preview()
    _dismiss_issue(idx)


def render_changes_tab() -> None:
    history = st.session_state['df_history']
    if not history:
        st.info("No changes yet. Apply a fix from the Issues tab to see it here.")
        return

    st.caption(f"{len(history)} change(s) applied.")

    # Newest first — like `git log`
    newest_idx = len(history) - 1
    for entry in reversed(history):
        auto_expand = entry['applied_at'] == newest_idx
        header = f"✅ {entry['label']}"
        if entry['log_entry']:
            col = entry['log_entry'].get('column')
            if col:
                header += f" — `{col}`"
        with st.expander(header, expanded=auto_expand):
            render_diff(entry['diff'])
            code = code_snippets.transform_code(entry['label'], entry.get('log_entry', {}))
            if code:
                with st.expander('📜 View pandas code'):
                    st.code(code, language='python')

    # Cumulative diff vs original
    if st.session_state.get('original_df') is not None:
        st.divider()
        st.subheader("Cumulative changes vs original")
        cumulative = compute_diff(st.session_state['original_df'], st.session_state['df'])
        render_diff(cumulative)




def render_custom_code_tab() -> None:
    """VS Code-inspired codespace: ACE editor, data explorer, package manager, terminal output."""
    import sys as _sys
    from utils.sandbox import run_code, install_package, get_sidebar_package_status

    df_current = st.session_state['df']
    df_rows, df_cols = df_current.shape

    # ══════════════════════════════════════════════════════════════════════════
    # CSS — VS Code dark theme panels
    # ══════════════════════════════════════════════════════════════════════════
    st.markdown("""
<style>
.cs-sidebar {
  background:#1e1e1e; border:1px solid #3c3c3c;
  border-radius:6px; padding:0; overflow:hidden;
}
.cs-section { border-bottom:1px solid #3c3c3c; padding:7px 10px 9px 10px; }
.cs-section-title {
  font-size:10px; font-weight:700; letter-spacing:.1em; color:#cccccc;
  text-transform:uppercase; margin-bottom:5px;
  font-family:'Menlo','Consolas','SF Mono',monospace;
}
.cs-stat { font-size:11px; color:#9cdcfe; margin-bottom:2px;
  font-family:'Menlo','Consolas','SF Mono',monospace; }
.cs-col-row {
  display:flex; align-items:center; gap:5px; padding:2px 0;
  font-size:11px; color:#d4d4d4;
  font-family:'Menlo','Consolas','SF Mono',monospace;
}
.cs-dtype {
  font-size:9px; background:#264f78; color:#9cdcfe;
  border-radius:3px; padding:1px 5px; white-space:nowrap; flex-shrink:0;
}
.cs-dtype-str  { background:#3a3a1e; color:#dcdcaa; }
.cs-dtype-bool { background:#1e3a2e; color:#4ec9b0; }
.cs-dtype-date { background:#2d1e3a; color:#c586c0; }
.cs-pkg-row {
  display:flex; align-items:center; gap:6px; padding:2px 0;
  font-size:11px; font-family:'Menlo','Consolas','SF Mono',monospace;
}
.cs-pkg-name { color:#9cdcfe; min-width:80px; }
.cs-pkg-ver  { color:#6a9955; font-size:10px; }
.cs-pkg-miss { color:#606060; font-size:10px; font-style:italic; }
.cs-dot-ok   { width:6px; height:6px; border-radius:50%;
  background:#4ec9b0; flex-shrink:0; display:inline-block; margin-top:1px; }
.cs-dot-miss { width:6px; height:6px; border-radius:50%;
  background:#606060; flex-shrink:0; display:inline-block; margin-top:1px; }
.cs-tab-bar {
  background:#2d2d2d; border:1px solid #3c3c3c; border-bottom:none;
  border-radius:6px 6px 0 0; display:flex; align-items:stretch;
  height:34px; overflow:hidden;
}
.cs-tab {
  display:flex; align-items:center; gap:6px; padding:0 16px;
  font-size:12px; color:#cccccc; background:#1e1e1e;
  border-top:2px solid #007acc; border-right:1px solid #3c3c3c;
  font-family:'Menlo','Consolas','SF Mono',monospace;
}
.cs-tab-close { color:#858585; margin-left:4px; }
.cs-terminal {
  background:#1e1e1e; border:1px solid #3c3c3c;
  border-top:none; border-radius:0 0 6px 6px; overflow:hidden;
  margin-top:0;
}
.cs-term-bar {
  background:#2d2d2d; border-bottom:1px solid #3c3c3c;
  padding:4px 12px; display:flex; align-items:center; gap:10px;
  font-size:11px; color:#858585;
  font-family:'Menlo','Consolas','SF Mono',monospace;
}
.cs-term-active { color:#ffffff; border-bottom:2px solid #007acc; padding-bottom:2px; }
.cs-term-body {
  padding:10px 14px; font-size:12px; line-height:1.7; color:#cccccc;
  min-height:80px; max-height:240px; overflow-y:auto;
  font-family:'Menlo','Consolas','SF Mono',monospace;
}
.t-ok   { color:#4ec9b0; }
.t-err  { color:#f44747; }
.t-info { color:#569cd6; }
.t-dim  { color:#606060; }
.t-out  { color:#d4d4d4; white-space:pre; }
.cs-status-bar {
  background:#007acc; border-radius:0 0 4px 4px; padding:2px 10px;
  font-size:11px; color:#fff; display:flex; gap:16px;
  font-family:'Menlo','Consolas','SF Mono',monospace;
}
</style>
""", unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════════
    # Sidebar HTML helpers
    # ══════════════════════════════════════════════════════════════════════════
    def _badge(dtype_str: str) -> str:
        s = str(dtype_str)
        if 'float' in s or 'int' in s:
            cls = 'cs-dtype'
        elif 'object' in s or 'str' in s or 'string' in s:
            cls = 'cs-dtype cs-dtype-str'
        elif 'bool' in s:
            cls = 'cs-dtype cs-dtype-bool'
        elif 'date' in s or 'time' in s:
            cls = 'cs-dtype cs-dtype-date'
        else:
            cls = 'cs-dtype'
        short = (s.replace('datetime64[ns]', 'dt64')
                  .replace('float64', 'f64').replace('float32', 'f32')
                  .replace('int64', 'i64').replace('int32', 'i32')
                  .replace('object', 'str').replace('boolean', 'bool'))
        return f'<span class="{cls}">{short}</span>'

    col_html = ''
    for c in df_current.columns[:35]:
        null_pct = int(df_current[c].isna().mean() * 100)
        null_tag = (f' <span style="color:#606060;font-size:10px">({null_pct}%)</span>'
                    if null_pct else '')
        col_html += (
            f'<div class="cs-col-row">{_badge(df_current[c].dtype)}'
            f'<span style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;'
            f'max-width:115px" title="{c}">{c}</span>{null_tag}</div>'
        )
    if len(df_current.columns) > 35:
        col_html += f'<div class="cs-col-row t-dim">… {len(df_current.columns)-35} more</div>'

    pkg_html = ''
    for p in get_sidebar_package_status():
        dot = '<span class="cs-dot-ok"></span>' if p['installed'] else '<span class="cs-dot-miss"></span>'
        ver = (f'<span class="cs-pkg-ver">{p["version"]}</span>' if p['installed']
               else '<span class="cs-pkg-miss">not installed</span>')
        pkg_html += (f'<div class="cs-pkg-row">{dot}'
                     f'<span class="cs-pkg-name">{p["name"]}</span>{ver}</div>')

    sidebar_html = f"""
<div class="cs-sidebar">
  <div class="cs-section">
    <div class="cs-section-title">⬡ Explorer</div>
    <div class="cs-stat">📄 dataset.csv</div>
    <div class="cs-stat" style="color:#6a9955">{df_rows:,} rows × {df_cols} columns</div>
  </div>
  <div class="cs-section">
    <div class="cs-section-title">⊞ Columns</div>
    {col_html}
  </div>
  <div class="cs-section">
    <div class="cs-section-title">📦 Packages</div>
    {pkg_html}
  </div>
</div>"""

    # ══════════════════════════════════════════════════════════════════════════
    # Two-column layout: sidebar | editor
    # ══════════════════════════════════════════════════════════════════════════
    sb_col, ed_col = st.columns([1, 3], gap='small')

    with sb_col:
        st.markdown(sidebar_html, unsafe_allow_html=True)

        with st.expander('📋 Snippets', expanded=False):
            snip_search = st.text_input('Filter snippets', placeholder='search…',
                                        key='_cs_snip_search', label_visibility='collapsed')
        st.markdown('**Install package**')
        pkg_input = st.text_input(
            'pkg', placeholder='e.g. scipy, nltk, plotly',
            key='_cs_pkg_input', label_visibility='collapsed',
        )
        if st.button('pip install', key='_cs_install_btn', use_container_width=True):
            if pkg_input.strip():
                with st.spinner(f'Installing {pkg_input}…'):
                    ok, msg = install_package(pkg_input.strip())
                (st.success if ok else st.error)(msg)
            else:
                st.warning('Enter a package name.')
        st.markdown(
            f'<div style="font-size:10px;color:#606060;margin-top:6px">'
            f'Python {_sys.version.split()[0]}</div>',
            unsafe_allow_html=True,
        )

    with ed_col:
        # Snippet buttons rendered inside the editor column so they live above the editor
        num_cols  = [c for c in df_current.columns if df_current[c].dtype in ('float64', 'int64', 'Int64')]
        str_cols  = [c for c in df_current.columns if str(df_current[c].dtype) in ('object', 'str')]
        any_col   = df_current.columns[0] if len(df_current.columns) else 'col'
        nc = num_cols[0] if num_cols else any_col
        sc = str_cols[0] if str_cols else any_col

        # ── Snippet categories (inside editor column) ─────────────────────────
        _SNIPPET_CATS = {
            'General': [
                ('Drop NaN rows',   f"df = df.dropna(subset=['{any_col}']).reset_index(drop=True)"),
                ('Drop duplicates', "df = df.drop_duplicates().reset_index(drop=True)"),
                ('Filter rows',     f"df = df[df['{any_col}'] != ''].reset_index(drop=True)"),
                ('Rename column',   f"df = df.rename(columns={{'{any_col}': 'new_name'}})"),
                ('Drop column',     f"df = df.drop(columns=['{any_col}'])"),
                ('Sort',            f"df = df.sort_values('{any_col}').reset_index(drop=True)"),
            ],
            'Text': [
                ('Strip spaces',    f"df['{sc}'] = df['{sc}'].str.strip()"),
                ('Lowercase',       f"df['{sc}'] = df['{sc}'].str.lower()"),
                ('Uppercase',       f"df['{sc}'] = df['{sc}'].str.upper()"),
                ('Replace',         f"df['{sc}'] = df['{sc}'].str.replace('old', 'new', regex=False)"),
                ('Extract pattern', f"df['{sc}'] = df['{sc}'].str.extract(r'(\\d+)')"),
                ('Contains filter', f"df = df[df['{sc}'].str.contains('val', na=False)]"),
            ],
            'Numeric': [
                ('Fill NaN median', f"df['{nc}'] = df['{nc}'].fillna(df['{nc}'].median())"),
                ('Fill NaN zero',   f"df['{nc}'] = df['{nc}'].fillna(0)"),
                ('Clip outliers',   f"df['{nc}'] = df['{nc}'].clip(lower=0)"),
                ('Cast to int',     f"df['{nc}'] = pd.to_numeric(df['{nc}'], errors='coerce').astype('Int64')"),
                ('Cast to float',   f"df['{nc}'] = df['{nc}'].astype('float64')"),
                ('New column ×2',   f"df['new_col'] = df['{nc}'] * 2"),
            ],
            'DateTime': [
                ('Parse datetime',  f"df['{any_col}'] = pd.to_datetime(df['{any_col}'], errors='coerce')"),
                ('Extract year',    f"df['year'] = df['{any_col}'].dt.year"),
                ('Extract month',   f"df['month'] = df['{any_col}'].dt.month"),
                ('Extract day',     f"df['day'] = df['{any_col}'].dt.day"),
                ('Format string',   f"df['{any_col}'] = df['{any_col}'].dt.strftime('%Y-%m-%d')"),
                ('Days since 2000', f"df['days'] = (df['{any_col}'] - pd.Timestamp('2000-01-01')).dt.days"),
            ],
            'Reshape': [
                ('Reset index',     "df = df.reset_index(drop=True)"),
                ('Groupby sum',     f"df = df.groupby('{any_col}').agg({{'{nc}': 'sum'}}).reset_index()"),
                ('Pivot table',     f"df = df.pivot_table(index='{any_col}', values='{nc}', aggfunc='mean').reset_index()"),
                ('Melt',            f"df = df.melt(id_vars=['{any_col}'], var_name='variable', value_name='value')"),
                ('Explode list col',f"df = df.explode('{any_col}').reset_index(drop=True)"),
                ('Bin numeric',     f"df['binned'] = pd.cut(df['{nc}'], bins=5, labels=False)"),
            ],
        }

        snip_search = st.session_state.get('_cs_snip_search', '')
        with st.expander('📋 Snippets — click to insert', expanded=False):
            for cat_name, cat_snippets in _SNIPPET_CATS.items():
                filtered = [s for s in cat_snippets
                            if not snip_search or snip_search.lower() in s[0].lower()]
                if not filtered:
                    continue
                st.markdown(f'**{cat_name}**')
                for row in [filtered[i:i+3] for i in range(0, len(filtered), 3)]:
                    cols_w = st.columns(len(row))
                    for col_widget, (label, snippet) in zip(cols_w, row):
                        if col_widget.button(label, key=f'snip_{cat_name}_{label}',
                                             use_container_width=True):
                            cur = st.session_state.get('_custom_code_text', '')
                            st.session_state['_custom_code_text'] = (
                                cur.rstrip('\n') + '\n' + snippet if cur.strip() else snippet
                            )
                            st.rerun()

        # ── VS Code tab bar ───────────────────────────────────────────────────
        st.markdown("""
<div class="cs-tab-bar">
  <div class="cs-tab">🐍 main.py <span class="cs-tab-close">×</span></div>
</div>""", unsafe_allow_html=True)

        # ── ACE editor ───────────────────────────────────────────────────────
        _DEFAULT = (
            f'# df  →  {df_rows:,} rows × {df_cols} columns\n'
            f'# Available: pd, np, and any installed package via import\n\n'
        )
        _themes = ['tomorrow_night_eighties', 'monokai', 'dracula',
                   'nord_dark', 'solarized_dark', 'tomorrow', 'github']
        theme = st.session_state.get('_cs_theme', 'tomorrow_night_eighties')

        st.caption('Ctrl+/ comment · Tab autocomplete · Ctrl+Z undo · Shift+Tab dedent')
        code = st_ace(
            value=st.session_state.get('_custom_code_text', _DEFAULT),
            language='python',
            theme=theme,
            keybinding='vscode',
            font_size=13,
            tab_size=4,
            show_gutter=True,
            show_print_margin=False,
            wrap=False,
            auto_update=True,
            height=340,
            key='_custom_code_editor',
        )
        if code is not None:
            st.session_state['_custom_code_text'] = code
        else:
            code = st.session_state.get('_custom_code_text', _DEFAULT)

        tc1, tc2 = st.columns([4, 1])
        chosen_theme = tc1.selectbox(
            'Theme', _themes,
            index=_themes.index(theme) if theme in _themes else 0,
            key='_cs_theme_pick', label_visibility='collapsed',
        )
        if chosen_theme != theme:
            st.session_state['_cs_theme'] = chosen_theme
            st.rerun()

        # ── Action bar ────────────────────────────────────────────────────────
        has_preview = '_custom_preview_df' in st.session_state
        bc1, bc2, bc3, bc4 = st.columns([2, 2, 1, 1])
        run_clicked   = bc1.button('▶  Run', key='custom_run_btn', type='primary',
                                   use_container_width=True)
        apply_clicked = bc2.button('✅  Apply', key='custom_apply_btn',
                                   disabled=not has_preview, use_container_width=True)
        if bc3.button('↺  Clear', key='custom_clear_btn', use_container_width=True):
            for k in ('_custom_preview_df', '_cs_result', '_custom_code_text'):
                st.session_state.pop(k, None)
            st.rerun()
        if bc4.button('⊘  Reset', key='custom_reset_btn', use_container_width=True):
            for k in list(st.session_state.keys()):
                if k.startswith('_custom_') or k.startswith('_cs_'):
                    st.session_state.pop(k, None)
            st.rerun()

        # ── Execute ───────────────────────────────────────────────────────────
        if run_clicked:
            st.session_state.pop('_custom_preview_df', None)
            stripped = (code or '').strip()
            if not stripped or all(ln.strip().startswith('#') for ln in stripped.splitlines()):
                st.session_state['_cs_result'] = {
                    'ok': False, 'error': 'No executable code found.',
                    'stdout': '', 'variables': {}, 'elapsed': 0, 'df': None,
                }
            else:
                result = run_code(stripped, df_current)
                st.session_state['_cs_result'] = result
                if result['ok']:
                    st.session_state['_custom_preview_df'] = result['df']
            st.rerun()

        # ── Terminal panel ────────────────────────────────────────────────────
        result = st.session_state.get('_cs_result')

        def _esc(s: str) -> str:
            return s.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

        lines: list[str] = []
        if result is None:
            lines.append(
                f'<span class="t-dim">Neatly Codespace  ·  '
                f'Python {_sys.version.split()[0]}  ·  '
                f'df: {df_rows:,} rows × {df_cols} cols ready</span>'
            )
        else:
            ms = result['elapsed'] * 1000
            if result['ok']:
                pf = result['df']
                r0, c0 = df_current.shape
                r1, c1 = pf.shape
                delta = []
                if r1 != r0:
                    delta.append(f'rows {r0:,} → {r1:,} ({"↓" if r1 < r0 else "↑"}{abs(r1-r0):,})')
                if c1 != c0:
                    delta.append(f'cols {c0} → {c1} ({"−" if c1 < c0 else "+"}{abs(c1-c0)})')
                lines.append(f'<span class="t-ok">✓ Execution complete ({ms:.0f} ms)</span>')
                if delta:
                    lines.append(f'<span class="t-info">  Shape: {" · ".join(delta)}</span>')
                else:
                    lines.append(f'<span class="t-dim">  Shape: unchanged ({r1:,} × {c1})</span>')
                if result.get('variables'):
                    vs = ', '.join(
                        f'{k}: {type(v).__name__}'
                        for k, v in list(result['variables'].items())[:6]
                    )
                    lines.append(f'<span class="t-dim">  Variables: {_esc(vs)}</span>')
            else:
                lines.append(f'<span class="t-err">✗ Error ({ms:.0f} ms)</span>')
                for ln in _esc(result['error']).splitlines():
                    lines.append(f'<span class="t-err">  {ln}</span>')

            if result.get('stdout'):
                lines.append('<span class="t-dim">── stdout ──────────────────────</span>')
                for ln in _esc(result['stdout']).splitlines():
                    lines.append(f'<span class="t-out">  {ln}</span>')

        body = '<br>'.join(lines)
        st.markdown(f"""
<div class="cs-terminal">
  <div class="cs-term-bar">
    <span class="cs-term-active">TERMINAL</span>
    <span>|</span><span>OUTPUT</span>
  </div>
  <div class="cs-term-body">{body}</div>
</div>""", unsafe_allow_html=True)

        st.markdown(
            f'<div class="cs-status-bar">'
            f'<span>🐍 Python {_sys.version.split()[0]}</span>'
            f'<span>df: {df_rows:,} × {df_cols}</span>'
            f'<span>UTF-8  LF</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

        # ── Preview diff ──────────────────────────────────────────────────────
        if '_custom_preview_df' in st.session_state and result and result['ok']:
            preview_df = st.session_state['_custom_preview_df']
            diff = compute_diff(df_current, preview_df)
            r0, c0 = df_current.shape
            r1, c1 = preview_df.shape
            parts = []
            if r1 != r0:
                parts.append(f"rows {r0:,} → {r1:,} ({'−' if r1<r0 else '+'}{abs(r1-r0):,})")
            if c1 != c0:
                parts.append(f"cols {c0} → {c1}")
            st.success('**Preview ready** — ' + (' · '.join(parts) if parts else 'no shape change'))
            render_diff(diff)
            added   = sorted(set(preview_df.columns) - set(df_current.columns))
            removed = sorted(set(df_current.columns) - set(preview_df.columns))
            if added:
                st.caption('New columns: ' + ', '.join(f'`{c}`' for c in added))
            if removed:
                st.caption('Removed: ' + ', '.join(f'`{c}`' for c in removed))
            with st.expander('🔍 Preview dataset (first 100 rows)', expanded=False):
                st.dataframe(preview_df.head(100), use_container_width=True)

        # ── Apply ─────────────────────────────────────────────────────────────
        if apply_clicked and '_custom_preview_df' in st.session_state:
            preview_df  = st.session_state.pop('_custom_preview_df')
            df_before   = df_current.copy()
            label       = 'Custom Code'
            applied_code = code or ''

            st.session_state['df'] = preview_df
            diff = compute_diff(df_before, preview_df)
            rows_affected = diff.get('rows_changed', 0) + diff.get('rows_removed', 0)

            log_entry = {
                'action':         'custom_code',
                'code':           applied_code,
                'rows_before':    len(df_before),
                'rows_after':     len(preview_df),
                'columns_before': len(df_before.columns),
                'columns_after':  len(preview_df.columns),
            }
            st.session_state['cleaning_log'].append(log_entry)
            log_event('decision_made', action='custom_code', rows_affected=rows_affected)

            history = st.session_state['df_history']
            history.append({
                'label': label, 'log_entry': log_entry,
                'df_before': df_before, 'diff': diff, 'applied_at': len(history),
            })
            if len(history) > _HISTORY_CAP:
                history.pop(0)

            undo_stack = st.session_state.setdefault('_undo_stack', [])
            undo_stack.append({'df': df_before, 'issue': {}, 'label': label})
            if len(undo_stack) > _UNDO_CAP:
                undo_stack.pop(0)

            parts = [f'**{label}** applied']
            if diff.get('rows_changed'):
                parts.append(f"{diff['rows_changed']} rows updated")
            if diff.get('rows_removed'):
                parts.append(f"{diff['rows_removed']} rows removed")
            st.session_state['_highlight_cols'] = diff.get('columns_affected', [])
            st.session_state['_last_action_feedback'] = ' — '.join(parts)
            st.rerun()


def render_custom_rules_tab() -> None:
    """Airtable-style inline rule builder with live row-count previews."""
    import re as _re
    import uuid

    df: pd.DataFrame = st.session_state['df']
    rules: list[dict] = st.session_state.setdefault('_custom_rules', [])
    columns = df.columns.tolist()

    # ── Styles ────────────────────────────────────────────────────────────────
    st.markdown("""
<style>
.cr-badge {
  display:inline-flex;align-items:center;justify-content:center;
  width:22px;height:22px;border-radius:50%;
  background:#1e3a5f;color:#60a5fa;font-size:11px;font-weight:700;flex-shrink:0;
}
.cr-empty {
  text-align:center;padding:32px 16px;color:#64748b;
  border:1px dashed #1e2d40;border-radius:8px;font-size:13px;
}
.cr-preview-hit   { font-size:13px;color:#4ec9b0;padding:4px 0; }
.cr-preview-miss  { font-size:13px;color:#fbbf24;padding:4px 0; }
.cr-preview-clean { font-size:13px;color:#6a9955;padding:4px 0; }
</style>""", unsafe_allow_html=True)

    # ── Rule type map ─────────────────────────────────────────────────────────
    _TYPES = {
        '🔤  Replace text':            'find_replace',
        '✏️  Fill empty cells':         'fill_missing',
        '📏  Keep numbers in range':    'clamp',
        '🗑️  Remove non-matching rows': 'drop_regex',
    }
    _DESCS = {
        'find_replace': 'Find a specific value in a column and replace it with something else.',
        'fill_missing': 'Replace blank / null cells with a default value you choose.',
        'clamp':        'Clamp numeric values so they stay within a min–max range.',
        'drop_regex':   'Drop rows whose value does not match a regular expression pattern.',
    }

    # ── Builder (no st.form — reactive, updates live as you type) ────────────
    st.markdown('#### Build a rule')

    c_type, c_col = st.columns([3, 2])
    rule_type_label = c_type.selectbox('What do you want to do?', list(_TYPES.keys()), key='_cr_type')
    rule_type = _TYPES[rule_type_label]
    column = c_col.selectbox('On which column?', columns, key='_cr_col')
    st.caption(_DESCS[rule_type])

    valid = True
    preview_html = ''
    params: dict = {}

    if rule_type == 'find_replace':
        c1, c2, c3 = st.columns([5, 5, 2])
        find_val    = c1.text_input('Find', placeholder='e.g.  N/A  or  ?  or  --', key='_cr_find')
        replace_val = c2.text_input('Replace with', placeholder='leave blank to clear the cell', key='_cr_replace')
        case_sens   = c3.checkbox('Aa', value=False, key='_cr_case', help='Match case-sensitively')
        if not find_val:
            valid = False
        else:
            params = {'find_value': find_val, 'replace_value': replace_val, 'case_sensitive': case_sens}
            if column in df.columns:
                s = df[column].astype(str)
                mask = (s == find_val) if case_sens else (s.str.lower() == find_val.lower())
                n = int(mask.sum())
                if n:
                    preview_html = f'<div class="cr-preview-hit">🎯 {n:,} row(s) match "{find_val}" in <strong>{column}</strong></div>'
                else:
                    preview_html = f'<div class="cr-preview-miss">⚠️ No rows match "{find_val}" in <strong>{column}</strong></div>'

    elif rule_type == 'fill_missing':
        fill_val = st.text_input('Fill empty cells with', placeholder='e.g.  Unknown  or  0  or  N/A', key='_cr_fill')
        if not fill_val:
            valid = False
        else:
            params = {'fill_value': fill_val}
            if column in df.columns:
                n = int(df[column].isna().sum())
                if n:
                    preview_html = f'<div class="cr-preview-hit">🎯 {n:,} empty cell(s) in <strong>{column}</strong> would be filled</div>'
                else:
                    preview_html = f'<div class="cr-preview-clean">✓ No empty cells in <strong>{column}</strong></div>'

    elif rule_type == 'clamp':
        if not pd.api.types.is_numeric_dtype(df[column]):
            st.warning(f'**{column}** is not numeric — choose a numeric column for this rule.')
            valid = False
        else:
            col_min = float(df[column].min()) if df[column].notna().any() else 0.0
            col_max = float(df[column].max()) if df[column].notna().any() else 100.0
            c1, c2 = st.columns(2)
            num_min = c1.number_input('Min value', value=col_min, key='_cr_min')
            num_max = c2.number_input('Max value', value=col_max, key='_cr_max')
            if num_min >= num_max:
                st.error('Min must be less than Max.')
                valid = False
            else:
                params = {'lo': float(num_min), 'hi': float(num_max)}
                mask = df[column].notna() & ((df[column] < num_min) | (df[column] > num_max))
                n = int(mask.sum())
                if n:
                    preview_html = f'<div class="cr-preview-hit">🎯 {n:,} value(s) outside [{num_min}, {num_max}] would be clamped</div>'
                else:
                    preview_html = f'<div class="cr-preview-clean">✓ All values already within [{num_min}, {num_max}]</div>'

    else:  # drop_regex
        regex_pat = st.text_input(
            'Regex pattern',
            placeholder=r'e.g.  ^\d{5}$  or  ^[A-Z]{2}\d+$',
            key='_cr_regex',
        )
        st.caption('Rows where the column does **not** match this pattern will be removed.')
        if not regex_pat:
            valid = False
        else:
            try:
                compiled = _re.compile(regex_pat)
                params = {'pattern': regex_pat, 'action': 'drop_rows'}
                if column in df.columns:
                    mask = ~df[column].astype(str).str.match(compiled, na=False)
                    n = int(mask.sum())
                    preview_html = f'<div class="cr-preview-hit">🗑️ {n:,} row(s) would be removed (non-matching)</div>'
            except _re.error as exc:
                st.error(f'Invalid pattern: {exc}')
                valid = False

    if preview_html:
        st.markdown(preview_html, unsafe_allow_html=True)

    if st.button('+ Add to Queue', key='_cr_add_btn', type='primary', disabled=not valid):
        if rule_type == 'find_replace':
            lbl = f'🔤 **{column}** · "{params["find_value"]}" → "{params["replace_value"] or "(blank)"}"'
        elif rule_type == 'fill_missing':
            lbl = f'✏️ **{column}** · fill empty → "{params["fill_value"]}"'
        elif rule_type == 'clamp':
            lbl = f'📏 **{column}** · clamp to [{params["lo"]}, {params["hi"]}]'
        else:
            lbl = f'🗑️ **{column}** · remove non-matching `{params["pattern"]}`'
        rules.append({'id': str(uuid.uuid4())[:8], 'type': rule_type,
                      'column': column, 'params': params, 'label': lbl})
        st.session_state['_custom_rules'] = rules
        st.rerun()

    # ── Queue ─────────────────────────────────────────────────────────────────
    st.markdown('---')

    if not rules:
        st.markdown('<div class="cr-empty">No rules queued yet.<br>Build one above and click <strong>+ Add to Queue</strong>.</div>', unsafe_allow_html=True)
        return

    # Approximate total rows affected (sequential simulation)
    total_affected = 0
    _tmp = df.copy()
    for _r in rules:
        _col, _p = _r['column'], _r['params']
        try:
            if _r['type'] == 'find_replace':
                _s = _tmp[_col].astype(str)
                _m = (_s == _p['find_value']) if _p.get('case_sensitive') else (_s.str.lower() == _p['find_value'].lower())
                total_affected += int(_m.sum())
            elif _r['type'] == 'fill_missing':
                total_affected += int(_tmp[_col].isna().sum())
            elif _r['type'] == 'clamp':
                _m = _tmp[_col].notna() & ((_tmp[_col] < _p['lo']) | (_tmp[_col] > _p['hi']))
                total_affected += int(_m.sum())
            else:
                _m = ~_tmp[_col].astype(str).str.match(_re.compile(_p['pattern']), na=False)
                total_affected += int(_m.sum())
        except Exception:
            pass

    n_rules = len(rules)
    st.markdown(
        f'#### Queue &nbsp;<span style="color:#60a5fa;font-size:14px">'
        f'({n_rules} rule{"s" if n_rules > 1 else ""} · ~{total_affected:,} rows affected)'
        f'</span>',
        unsafe_allow_html=True,
    )

    for i, rule in enumerate(rules):
        c_num, c_lbl, c_up, c_dn, c_rm = st.columns([1, 10, 1, 1, 2])
        c_num.markdown(f'<div class="cr-badge">{i + 1}</div>', unsafe_allow_html=True)
        c_lbl.markdown(rule['label'])
        if n_rules > 1:
            if c_up.button('↑', key=f'_cr_up_{rule["id"]}', disabled=(i == 0)):
                rules.insert(i - 1, rules.pop(i))
                st.session_state['_custom_rules'] = rules
                st.rerun()
            if c_dn.button('↓', key=f'_cr_dn_{rule["id"]}', disabled=(i == n_rules - 1)):
                rules.insert(i + 1, rules.pop(i))
                st.session_state['_custom_rules'] = rules
                st.rerun()
        if c_rm.button('✕ Remove', key=f'_cr_rm_{rule["id"]}', use_container_width=True):
            rules.pop(i)
            st.session_state['_custom_rules'] = rules
            st.rerun()

    st.divider()
    col_apply, col_clear = st.columns([3, 1])
    if col_apply.button(
        f'▶  Apply All  ({total_affected:,} rows)',
        type='primary', use_container_width=True, key='_cr_apply',
    ):
        df_before  = df.copy()
        current_df = df.copy()
        log = st.session_state['cleaning_log']
        errors: list[str] = []

        for idx_r, rule in enumerate(rules):
            _col, _p = rule['column'], rule['params']
            try:
                if rule['type'] == 'find_replace':
                    current_df = find_replace(current_df, log, _col, **_p)
                elif rule['type'] == 'fill_missing':
                    current_df = fill_with_constant(current_df, log, _col, **_p)
                elif rule['type'] == 'clamp':
                    current_df = clip_to_range(current_df, log, _col, **_p)
                else:
                    current_df = apply_custom_regex(current_df, log, _col, **_p)
            except Exception as exc:
                errors.append(f'Rule {idx_r + 1}: {exc}')

        if errors:
            st.error('Some rules failed:\n' + '\n'.join(f'• {e}' for e in errors))
        else:
            diff = compute_diff(df_before, current_df)
            history = st.session_state['df_history']
            history.append({
                'label': f'Custom Rules ({n_rules})',
                'log_entry': {'action': 'custom_rules', 'rules_applied': n_rules},
                'df_before': df_before, 'diff': diff, 'applied_at': len(history),
            })
            if len(history) > _HISTORY_CAP:
                history.pop(0)
            undo_stack = st.session_state.setdefault('_undo_stack', [])
            undo_stack.append({'df': df_before, 'issue': {}, 'label': f'Custom Rules ({n_rules})'})
            if len(undo_stack) > _UNDO_CAP:
                undo_stack.pop(0)
            rows_aff = diff.get('rows_changed', 0) + diff.get('rows_removed', 0)
            st.session_state['df'] = current_df
            st.session_state['_custom_rules'] = []
            log_event('decision_made', action='custom_rules', rows_affected=rows_aff)
            st.session_state['_last_action_feedback'] = (
                f'**{n_rules} rule(s) applied** — {rows_aff} rows affected'
            )
            st.rerun()

    if col_clear.button('Clear Queue', key='_cr_clear', use_container_width=True):
        st.session_state['_custom_rules'] = []
        st.rerun()


def _dismiss_issue(idx: int) -> None:
    st.session_state['issues'].pop(idx)
    st.rerun()


def _humanize(s: str) -> str:
    return s.replace('_', ' ').title()

# ---------------------------------------------------------------------------
# Stage: done — helpers
# ---------------------------------------------------------------------------

_PUSH_DB_TYPES = ['PostgreSQL', 'MySQL', 'MySQL Workbench (Local)', 'SQLite']
_PUSH_DB_DEFAULTS = {
    'PostgreSQL': {'host': 'localhost', 'port': 5432},
    'MySQL': {'host': 'localhost', 'port': 3306},
    'MySQL Workbench (Local)': {'host': 'localhost', 'port': 3306},
    'SQLite': {},
}


def _render_push_to_db(df: pd.DataFrame, prefill: dict | None = None) -> None:
    """Render the Push to Database form.

    prefill: optional dict (db_type, host, port, user, password, database, or path for SQLite)
             sourced from the original connection so fields are pre-populated.
    """
    p = prefill or {}
    prefill_type = p.get('db_type', 'MySQL')
    type_index = _PUSH_DB_TYPES.index(prefill_type) if prefill_type in _PUSH_DB_TYPES else 0

    db_type = st.selectbox('Database type', _PUSH_DB_TYPES, index=type_index, key='push_db_type')
    _dialect = 'MySQL' if db_type == 'MySQL Workbench (Local)' else db_type

    if db_type == 'SQLite':
        db_path = st.text_input('SQLite file path', value=p.get('path', ''), placeholder='/path/to/file.db', key='push_sqlite_path')
        host = port = user = password = database = None
    else:
        defaults = _PUSH_DB_DEFAULTS[db_type]
        col1, col2 = st.columns([3, 1])
        host = col1.text_input('Host', value=p.get('host', defaults.get('host', '')), key='push_host')
        port = col2.number_input('Port', value=int(p.get('port', defaults.get('port', 5432))), min_value=1, max_value=65535, key='push_port')
        database = st.text_input('Database', value=p.get('database', ''), key='push_database')
        col3, col4 = st.columns(2)
        user = col3.text_input('User', value=p.get('user', ''), key='push_user')
        password = col4.text_input('Password', value=p.get('password', ''), type='password', key='push_password')
        db_path = None

    source_table = st.session_state.get('_db_source_table', '')
    default_table = f'{source_table}_cleaned' if source_table else ''
    table_name = st.text_input('Destination table name', value=default_table, key='push_table_name')
    write_mode = st.radio(
        'If table exists',
        ['Append', 'Replace'],
        horizontal=True,
        key='push_write_mode',
    )

    if st.button('Push cleaned data', key='push_db_btn', type='primary'):
        if not table_name:
            st.error('Enter a destination table name.')
            return
        try:
            if db_type == 'SQLite':
                if not db_path:
                    st.error('Enter a SQLite file path.')
                    return
                conn_str = build_connection_string('SQLite', path=db_path)
            else:
                conn_str = build_connection_string(
                    _dialect,
                    host=host,
                    port=int(port),
                    database=database,
                    user=user,
                    password=password,
                )
            rows_written = write_table(conn_str, df, table_name, if_exists=write_mode.lower())
            st.success(f'{rows_written:,} rows written to `{table_name}`.')
            log_event('export_pushed', db_type=db_type, table=table_name, rows=rows_written)
        except Exception as exc:
            st.error(f'Push failed: {exc}')


# ---------------------------------------------------------------------------
# Stage: done
# ---------------------------------------------------------------------------

def render_done() -> None:
    _render_stage_bar('done')

    # Success state hero
    st.markdown(
        """
        <div style="text-align: center; margin: 2rem 0 3rem 0;">
            <div style="font-size: 4rem; margin-bottom: 1rem;">✓</div>
            <h1 style="margin: 0; font-size: 2.5rem; color: #34d399;">All clean.</h1>
            <p style="margin: 0.5rem 0 0 0; font-size: 1.1rem; color: #71717a;">Your dataset is ready to use.</p>
        </div>
        """,
        unsafe_allow_html=True
    )

    df = st.session_state['df']
    original_df = st.session_state['original_df']
    cleaning_log = st.session_state['cleaning_log']

    # Metrics
    col1, col2, col3 = st.columns(3)
    col1.metric('Original Rows', f'{len(original_df):,}')
    removed = len(original_df) - len(df)
    col2.metric('Cleaned Rows', f'{len(df):,}', delta=f'-{removed:,} rows removed' if removed else None)
    col3.metric('Transforms Applied', len(cleaning_log))

    st.divider()
    st.subheader('Cleaned Data Preview')
    st.dataframe(df.head(20), use_container_width=True)

    st.divider()
    csv_bytes = df.to_csv(index=False).encode('utf-8')
    log_bytes = json.dumps(cleaning_log, indent=2, default=str).encode('utf-8')

    input_source = st.session_state.get('_input_source', 'file')
    source_config = st.session_state.get('_db_source_config')

    if input_source == 'database' and source_config:
        # DB source: lead with save-back tab, downloads secondary
        st.subheader('Export')
        tab_db, tab_dl = st.tabs(['💾 Save to Database', '📥 Download Files'])
        with tab_db:
            _render_push_to_db(df, prefill=source_config)
        with tab_dl:
            col1, col2 = st.columns(2)
            if col1.download_button('📥 Download Cleaned CSV', csv_bytes, 'cleaned_data.csv', 'text/csv', use_container_width=True):
                log_event('export_downloaded', export_type='csv', rows=len(df), columns=len(df.columns))
            if col2.download_button('📄 Download Log (JSON)', log_bytes, 'cleaning_log.json', 'application/json', use_container_width=True):
                log_event('export_downloaded', export_type='log', n_transforms=len(cleaning_log))
    else:
        # File source: downloads first, DB push optional
        st.subheader('Downloads')
        col1, col2 = st.columns(2)
        if col1.download_button('📥 Download Cleaned CSV', csv_bytes, 'cleaned_data.csv', 'text/csv', use_container_width=True):
            log_event('export_downloaded', export_type='csv', rows=len(df), columns=len(df.columns))
        if col2.download_button('📄 Download Log (JSON)', log_bytes, 'cleaning_log.json', 'application/json', use_container_width=True):
            log_event('export_downloaded', export_type='log', n_transforms=len(cleaning_log))

        st.divider()
        with st.expander('Save to Database (optional)', expanded=False):
            _render_push_to_db(df)

    if cleaning_log:
        with st.expander(f'View cleaning log ({len(cleaning_log)} transform{"s" if len(cleaning_log) != 1 else ""})', expanded=False):
            st.json(cleaning_log)

    st.divider()
    if st.button('Start Over', key='restart_btn'):
        log_event('session_abandoned', n_actions=len(cleaning_log))
        _reset_to_upload()
        st.rerun()


def _reset_to_upload() -> None:
    st.session_state['df'] = None
    st.session_state['original_df'] = None
    st.session_state['issues'] = []
    st.session_state['cleaning_log'] = []
    st.session_state['df_history'] = []
    st.session_state['column_contexts'] = []
    st.session_state['_undo_stack'] = []
    for _key in ('_upload_cache_key', '_upload_cached_df', '_db_loaded_df',
                 '_db_conn_str', '_db_tables', '_db_connected_type',
                 '_db_source_config', '_db_source_table', '_input_source'):
        st.session_state.pop(_key, None)
    st.session_state['_highlight_cols'] = []
    st.session_state['_last_action_label'] = ''
    st.session_state['stage'] = 'upload'
    _clear_preview()

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

st.markdown(
    '<p style="text-align:center;color:#64748b;font-size:12px;margin-top:2rem;">'
    '© 2026 Kunpeng Liu · <a href="mailto:liukunpeng267@gmail.com" style="color:#64748b;">liukunpeng267@gmail.com</a>'
    ' · Last updated April 19, 2026'
    '</p>',
    unsafe_allow_html=True,
)
