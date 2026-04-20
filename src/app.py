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
    coerce_to_numeric,
    drop_column,
    drop_duplicates,
    drop_invalid_rows,
    drop_missing,
    drop_non_numeric_rows,
    drop_out_of_range_rows,
    drop_whitespace_rows,
    fill_missing,
    clip_to_range,
    flag_invalid_patterns,
    flag_near_duplicates,
    merge_near_duplicates,
    normalize_text,
    null_out_whitespace,
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
)
from utils.analytics import init_session, log_event
from utils.session_state import init_state

load_dotenv()

_HISTORY_CAP = 20

# ---------------------------------------------------------------------------
# Session state initialisation
# ---------------------------------------------------------------------------

init_state()
init_session()   # generates session_id and fires session_started once per browser session

# ---------------------------------------------------------------------------
# Stage: upload
# ---------------------------------------------------------------------------

def render_upload() -> None:
    st.header('Load your data')
    st.write("Neatly will find data-quality issues, explain them in plain English, and let you clean the data with a few clicks.")

    tab_file, tab_db = st.tabs(['📁 File Upload', '🗄️ Database'])

    # --- File Upload Tab ---
    with tab_file:
        uploaded_file = st.file_uploader(
            'Choose a file (CSV, TSV, JSON, Excel, Parquet)',
            type=['csv', 'tsv', 'json', 'xlsx', 'xls', 'parquet'],
            key='file_uploader',
        )

        if not uploaded_file:
            return
        try:
            df = parse_uploaded_file(uploaded_file)
        except Exception as e:
            st.error(f'Error reading file: {e}')
            return

        _MAX_ROWS, _MAX_COLS = 500_000, 500
        if len(df) > _MAX_ROWS:
            st.error(f'File has {len(df):,} rows — limit is {_MAX_ROWS:,}. Upload a smaller sample.')
            return
        if len(df.columns) > _MAX_COLS:
            st.error(f'File has {len(df.columns)} columns — limit is {_MAX_COLS}.')
            return

        st.success(f'Loaded {len(df):,} rows, {len(df.columns)} columns')
        st.dataframe(df.head(10), use_container_width=True)

        if st.button('Start Diagnosis', key='diagnose_file_btn', type='primary'):
            log_event('file_uploaded', rows=len(df), columns=len(df.columns), source='file')
            st.session_state['original_df'] = df.copy()
            st.session_state['df'] = df
            st.session_state['issues'] = []
            st.session_state['cleaning_log'] = []
            st.session_state['df_history'] = []
            st.session_state['stage'] = 'diagnose'
            _clear_preview()
            st.rerun()

    # --- Database Tab ---
    with tab_db:
        _render_database_loader()


def _render_database_loader() -> None:
    """Render the database connection UI."""
    st.subheader('Connect to Database')

    col1, col2 = st.columns(2)
    with col1:
        db_type = st.selectbox(
            'Database Type',
            ['PostgreSQL', 'MySQL', 'SQLite', 'SQL Server'],
            key='db_type_select',
        )

    # Connection parameters based on DB type
    if db_type == 'SQLite':
        db_path = st.text_input('Database File Path', key='sqlite_path')
        if not st.button('Connect & Load', key='connect_sqlite_btn'):
            return
        try:
            conn_str = build_connection_string('SQLite', path=db_path)
            engine = create_connection(conn_str)
            tables = list_tables(engine)
            if not tables:
                st.warning('No tables found in database')
                return
            table_name = st.selectbox('Select Table', tables, key='sqlite_table')
            df = load_table(conn_str, table_name)
            _finalize_database_load(df)
        except Exception as e:
            st.error(f'Connection failed: {e}')
    else:
        # Network databases
        col1, col2 = st.columns(2)
        with col1:
            host = st.text_input('Host', key=f'{db_type}_host')
            port = st.number_input('Port', value=_get_default_port(db_type), key=f'{db_type}_port')
        with col2:
            database = st.text_input('Database', key=f'{db_type}_database')

        col1, col2 = st.columns(2)
        with col1:
            user = st.text_input('Username', key=f'{db_type}_user')
        with col2:
            password = st.text_input('Password', type='password', key=f'{db_type}_password')

        row_limit = st.slider('Row Limit', 100, 100000, 10000, step=1000)

        if st.button('Connect & Load', key=f'connect_{db_type}_btn'):
            try:
                conn_str = build_connection_string(
                    db_type,
                    host=host,
                    port=int(port),
                    database=database,
                    user=user,
                    password=password,
                )
                engine = create_connection(conn_str)
                tables = list_tables(engine)

                if not tables:
                    st.warning('No tables found in database')
                    return

                # Let user choose between table or custom query
                load_mode = st.radio('Load Mode', ['Select Table', 'Custom Query'], horizontal=True)

                if load_mode == 'Select Table':
                    table_name = st.selectbox('Select Table', tables)
                    df = load_table(conn_str, table_name, limit=row_limit)
                else:
                    sql_query = st.text_area('SQL Query', height=100)
                    if not sql_query.strip():
                        st.info('Enter a SQL SELECT query')
                        return
                    df = load_query(conn_str, sql_query, limit=row_limit)

                _finalize_database_load(df)
            except Exception as e:
                st.error(f'Connection failed: {e}')


def _finalize_database_load(df: pd.DataFrame) -> None:
    """Show preview and start diagnosis for database-loaded data."""
    st.success(f'Loaded {len(df):,} rows, {len(df.columns)} columns')
    st.dataframe(df.head(10), use_container_width=True)

    if st.button('Start Diagnosis', key='diagnose_db_btn', type='primary'):
        log_event('file_uploaded', rows=len(df), columns=len(df.columns), source='database')
        st.session_state['original_df'] = df.copy()
        st.session_state['df'] = df
        st.session_state['issues'] = []
        st.session_state['cleaning_log'] = []
        st.session_state['df_history'] = []
        st.session_state['stage'] = 'diagnose'
        _clear_preview()
        st.rerun()


def _get_default_port(db_type: str) -> int:
    """Get the default port for a database type."""
    ports = {'PostgreSQL': 5432, 'MySQL': 3306, 'SQL Server': 1433}
    return ports.get(db_type, 5432)

# ---------------------------------------------------------------------------
# Stage: diagnose
# ---------------------------------------------------------------------------

def render_diagnose() -> None:
    st.header('Diagnosing your data…')
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

_STATS_HIDE_KEYS = {
    'type', 'column', 'columns', 'sub_type', 'explanation', 'summary',
    'sample_values', 'example_values', 'sample_indices', 'row_indices',
    'dtype', 'detector', 'severity', 'sample_data', 'actions',
}


def render_decide() -> None:
    st.header('Review & Fix Issues')
    issues = st.session_state['issues']
    history = st.session_state['df_history']

    failed = st.session_state.pop('_failed_detectors', None)
    if failed:
        st.warning(
            f"⚠️ {len(failed)} detector(s) could not run and were skipped: "
            f"`{'`, `'.join(failed)}`. Results may be incomplete."
        )

    changes_label = f"📋 Changes ({len(history)})" if history else "📋 Changes"
    tab_issues, tab_changes = st.tabs(['🔍 Issues', changes_label])

    with tab_issues:
        _render_column_context_panel()
        if not issues:
            st.info('No issues found — your data looks clean.')
            if st.button('Finish', key='finish_btn', type='primary'):
                log_event("session_completed", n_actions=len(st.session_state["cleaning_log"]), issues_remaining=0)
                st.session_state["stage"] = "done"
                st.rerun()
        else:
            st.caption(f"{len(issues)} issue(s) remaining.")
            for i, issue in enumerate(issues):
                _render_issue_card(i, issue)

            st.divider()
            col1, col2 = st.columns(2)
            if col1.button('Done Reviewing', key='done_review_btn', type='primary'):
                log_event("session_completed", n_actions=len(st.session_state["cleaning_log"]), issues_remaining=len(issues))
                st.session_state["stage"] = "done"
                st.rerun()
            if col2.button('Start Over', key='restart_decide_btn'):
                _reset_to_upload()
                st.rerun()

    with tab_changes:
        render_changes_tab()


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

        snippet = code_snippets.DETECTION_SNIPPETS.get(issue_type)
        if snippet:
            with st.expander('🔍 How was this detected?'):
                formatted_snippet = snippet.format(col=column) if column else snippet
                st.code(formatted_snippet, language='python')

        actions = _actions_for(issue)
        if not actions:
            st.caption("_No automatic fix available for this issue._")
            return
        # Layout: one column per action + Preview + Skip
        cols = st.columns(len(actions) + 2)
        for btn_col, (label, handler) in zip(cols, actions):
            if btn_col.button(label, key=f'act_{idx}_{label}'):
                _apply_action(idx, handler, label)
        if cols[-2].button('👁 Preview', key=f'prev_{idx}'):
            # Preview the first action by default — user can click any specific
            # action to apply it; preview gives a read-only look at #1.
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

        if st.session_state.get('_preview_idx') == idx:
            _render_preview_panel(idx)


def _render_preview_panel(idx: int) -> None:
    handler = st.session_state['_preview_handler']
    label = st.session_state['_preview_label']
    try:
        df_preview = handler(st.session_state['df'].copy(), [])
    except Exception as e:
        st.error(f'Preview failed: {e}')
        return
    diff = compute_diff(st.session_state['df'], df_preview)
    st.info(f"**Preview — `{label}`.** These changes will apply if you click the action button above.")
    render_diff(diff)
    c1, c2 = st.columns([1, 5])
    if c1.button('Close preview', key=f'prev_close_{idx}'):
        _clear_preview()
        st.rerun()


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
                ('Clip to range', lambda df, log: clip_to_range(df, log, col, lo, hi)),
                ('Drop invalid rows', lambda df, log: drop_out_of_range_rows(df, log, col, lo, hi)),
            ]

    if issue_type == 'constant_column' and col:
        return [('Drop column', lambda df, log: drop_column(df, col, log))]

    if issue_type == 'id_column' and col:
        return [('Drop column', lambda df, log: drop_column(df, col, log))]

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
    log_event(
        'decision_made',
        action=label,
        issue_type=issue.get('type'),
        column=(issue.get('columns') or [None])[0],
        rows_affected=diff.get('rows_changed', 0) + diff.get('rows_removed', 0),
    )

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
    if col1.download_button('Download cleaned CSV', csv_bytes, 'cleaned_data.csv', 'text/csv'):
        log_event('export_downloaded', export_type='csv', rows=len(df), columns=len(df.columns))
    if col2.download_button('Download cleaning log (JSON)', log_bytes, 'cleaning_log.json', 'application/json'):
        log_event('export_downloaded', export_type='log', n_transforms=len(cleaning_log))

    if cleaning_log:
        st.subheader('Cleaning log')
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
