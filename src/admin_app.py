"""Internal analytics dashboard for Neatly AI — standalone, NOT deployed with the public app.

Run locally when you want to review analytics:
    streamlit run src/admin_app.py --server.port 8502

This file is deliberately NOT inside `src/pages/` — Streamlit auto-discovers
`pages/` and exposes every file there in the main app's sidebar. Keeping
this at `src/` level hides it from the public deployment entirely.

Password gate:
  - Set ADMIN_PASSWORD in a local `.streamlit/secrets.toml` (or Streamlit
    secrets, if you deploy this as its own app later). Without the secret
    the page is open — fine for solo local dev.
  - Once authenticated in a browser session, credentials are cached in
    session_state so widget interactions don't re-prompt.

Log source:
  - Reads `neatly_logs.jsonl` in cwd (local) or `/tmp/neatly_logs.jsonl`
    (cloud). Local runs see local dev sessions only — cloud logs are not
    synced back yet.

Streamlit Cloud deployment (separate app):
  - Point share.streamlit.io at this repo, set main file = src/admin_app.py
  - IMPORTANT: set ADMIN_PASSWORD in Streamlit secrets before deploying
  - NOTE: Cloud admin app reads its own /tmp — it cannot see the public
    app's logs without external log storage (Supabase, Upstash, etc.)
"""
import hmac
import json
from collections import Counter
from datetime import datetime

import altair as alt
import pandas as pd
import streamlit as st

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from utils.analytics import load_logs

st.set_page_config(page_title='Neatly Admin', page_icon='📊', layout='wide')

init_state_module = __import__('utils.session_state', fromlist=['init_state'])
init_state_module.init_state()

st.markdown("""
<style>
:root {
  --bg-primary: #0f0f11;
  --bg-secondary: #18181b;
  --text-primary: #f4f4f5;
  --text-muted: #71717a;
  --border: #27272a;
  --accent: #7c3aed;
}

[data-theme="light"] {
  --bg-primary: #fafafa;
  --bg-secondary: #ffffff;
  --text-primary: #1a1a1a;
  --text-muted: #71717a;
  --border: #e5e5e7;
  --accent: #6d28d9;
}

.block-container { max-width: 1200px; padding: 2rem 1rem; }
body { background: var(--bg-primary); color: var(--text-primary); font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; transition: background 0.3s, color 0.3s; }
h1, h2, h3, h4, h5, h6 { font-weight: 600; line-height: 1.3; }

.analytics-card { background: var(--bg-secondary); border: 1px solid var(--border); border-radius: 8px; padding: 1.5rem; margin-bottom: 1.5rem; }

[data-testid="metric-container"] { background: var(--bg-secondary); border: 1px solid var(--border); border-radius: 8px; padding: 1.25rem; }

[role="tablist"] button { font-weight: 600; }
.stButton > button { border-radius: 6px; font-weight: 600; }

[data-testid="stAppViewContainer"] { padding-top: 0; }
footer { display: none; }
</style>
""", unsafe_allow_html=True)

theme = st.session_state.get('theme', 'dark')
st.markdown(f"<script>document.documentElement.setAttribute('data-theme', '{theme}');</script>", unsafe_allow_html=True)

col1, col2 = st.columns([10, 1])
with col2:
    icon = '☀️' if theme == 'dark' else '🌙'
    label = 'Light' if theme == 'dark' else 'Dark'
    if st.button(f'{icon} {label}', key='admin_theme_toggle', use_container_width=False):
        st.session_state['theme'] = 'light' if theme == 'dark' else 'dark'
        st.rerun()

# ---------------------------------------------------------------------------
# Password gate
# ---------------------------------------------------------------------------

def _check_password() -> bool:
    try:
        expected = st.secrets.get('ADMIN_PASSWORD')
    except Exception:
        expected = None

    if not expected:
        return True

    if st.session_state.get('_admin_authed'):
        return True

    st.title('📊 Neatly Admin — Sign in')
    with st.form('admin_signin', clear_on_submit=False):
        pwd = st.text_input('Password', type='password', key='_admin_pwd_input')
        submitted = st.form_submit_button('Sign in')

    if not submitted:
        return False

    if hmac.compare_digest(pwd or '', expected):
        st.session_state['_admin_authed'] = True
        st.rerun()
    else:
        st.error('Incorrect password.')
    return False


if not _check_password():
    st.stop()

st.title('📊 Neatly Analytics')

# ---------------------------------------------------------------------------
# Chart helper — horizontal bar chart with readable left-side labels
# ---------------------------------------------------------------------------

_ACCENT = '#7c3aed'


def _hbar(data: pd.Series, x_label: str, color: str = _ACCENT) -> alt.Chart:
    """Return a horizontal Altair bar chart. Labels are on the y-axis (left)
    so they are always readable regardless of length."""
    df = data.reset_index()
    df.columns = ['Category', x_label]
    chart_height = max(160, len(data) * 38)
    return (
        alt.Chart(df)
        .mark_bar(color=color, cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
        .encode(
            y=alt.Y('Category:N', sort='-x', title=None,
                    axis=alt.Axis(labelLimit=240, labelFontSize=13)),
            x=alt.X(f'{x_label}:Q', title=x_label,
                    axis=alt.Axis(tickMinStep=1, labelFontSize=12)),
            tooltip=[
                alt.Tooltip('Category:N', title='Category'),
                alt.Tooltip(f'{x_label}:Q', title=x_label),
            ],
        )
        .properties(height=chart_height)
        .configure_axis(grid=False)
        .configure_view(strokeWidth=0)
    )


# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

@st.cache_data(ttl=10)
def _load() -> pd.DataFrame:
    entries = load_logs()
    if not entries:
        return pd.DataFrame()
    df = pd.DataFrame(entries)
    df['ts'] = pd.to_datetime(df['ts'], errors='coerce')
    df['date'] = df['ts'].dt.date
    return df


logs = _load()

if logs.empty:
    st.info('No events logged yet. Use the app to generate data.')
    st.stop()

if st.button('🔄 Refresh'):
    st.cache_data.clear()
    st.rerun()

# ---------------------------------------------------------------------------
# Overview metrics
# ---------------------------------------------------------------------------

sessions = logs['session_id'].nunique()
total_events = len(logs)
uploads = logs[logs['event'] == 'file_uploaded']
completions = logs[logs['event'] == 'session_completed']
decisions = logs[logs['event'] == 'decision_made']
skips = logs[logs['event'] == 'issue_skipped']
completion_rate = round(len(completions) / sessions * 100, 1) if sessions else 0

st.markdown('<div class="analytics-card">', unsafe_allow_html=True)
st.subheader('Overview')
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric('Sessions', sessions)
c2.metric('File Uploads', len(uploads))
c3.metric('Decisions Made', len(decisions))
c4.metric('Issues Skipped', len(skips))
c5.metric('Completion Rate', f'{completion_rate}%')
st.markdown('</div>', unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Conversion funnel
# ---------------------------------------------------------------------------

st.markdown('<div class="analytics-card">', unsafe_allow_html=True)
st.subheader('Conversion Funnel')

funnel_events = [
    ('session_started',     'Session started'),
    ('file_uploaded',       'File uploaded'),
    ('diagnosis_completed', 'Diagnosis completed'),
    ('decision_made',       'Action taken'),
    ('session_completed',   'Session completed'),
]

funnel_rows = []
for event, label in funnel_events:
    count = logs[logs['event'] == event]['session_id'].nunique()
    funnel_rows.append({'Step': label, 'Sessions': count})

funnel_df = pd.DataFrame(funnel_rows)
funnel_series = funnel_df.set_index('Step')['Sessions']

col_chart, col_table = st.columns([3, 1])
with col_chart:
    st.altair_chart(_hbar(funnel_series, 'Sessions'), use_container_width=True)
with col_table:
    st.caption('Drop-off rates')
    top = funnel_rows[0]['Sessions'] or 1
    table_rows = [
        {'Step': r['Step'], '% of start': f"{r['Sessions'] / top * 100:.0f}%"}
        for r in funnel_rows
    ]
    st.dataframe(pd.DataFrame(table_rows).set_index('Step'), use_container_width=True)

st.markdown('</div>', unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Issue types detected
# ---------------------------------------------------------------------------

st.markdown('<div class="analytics-card">', unsafe_allow_html=True)
st.subheader('Issue Types Detected')

detected = logs[logs['event'] == 'diagnosis_completed']
if not detected.empty and 'issue_types' in detected.columns:
    all_types: list[str] = []
    for val in detected['issue_types'].dropna():
        if isinstance(val, list):
            all_types.extend(val)
        elif isinstance(val, str):
            try:
                all_types.extend(json.loads(val))
            except Exception:
                all_types.append(val)
    if all_types:
        type_counts = pd.Series(Counter(all_types)).sort_values(ascending=False)
        st.altair_chart(_hbar(type_counts, 'Count'), use_container_width=True)
        st.caption(f'{len(type_counts)} unique issue type(s) across {len(detected)} diagnosis event(s)')
    else:
        st.caption('No issue type data yet.')
else:
    st.caption('No diagnosis events logged yet.')
st.markdown('</div>', unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Action preferences
# ---------------------------------------------------------------------------

st.markdown('<div class="analytics-card">', unsafe_allow_html=True)
st.subheader('Most Applied Actions')

if not decisions.empty and 'action' in decisions.columns:
    action_counts = decisions['action'].value_counts().head(10)
    st.altair_chart(_hbar(action_counts, 'Times Applied'), use_container_width=True)
    st.caption(f'{len(decisions)} total decisions across {decisions["session_id"].nunique()} session(s)')
else:
    st.caption('No decision events logged yet.')
st.markdown('</div>', unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Drop-off analysis
# ---------------------------------------------------------------------------

st.markdown('<div class="analytics-card">', unsafe_allow_html=True)
st.subheader('Where Do Users Drop Off?')
st.caption('Last recorded event per session — shows where sessions ended.')

last_events = (
    logs.sort_values('ts')
    .groupby('session_id')['event']
    .last()
    .value_counts()
)
if not last_events.empty:
    st.altair_chart(_hbar(last_events, 'Sessions'), use_container_width=True)
st.markdown('</div>', unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Dataset profile
# ---------------------------------------------------------------------------

st.markdown('<div class="analytics-card">', unsafe_allow_html=True)
st.subheader('Dataset Profile (Uploaded Files)')

if not uploads.empty:
    col1, col2 = st.columns(2)
    if 'rows' in uploads.columns:
        col1.metric('Avg Rows', f"{uploads['rows'].dropna().mean():,.0f}")
    if 'columns' in uploads.columns:
        col2.metric('Avg Columns', f"{uploads['columns'].dropna().mean():.0f}")
    if 'rows' in uploads.columns and 'columns' in uploads.columns:
        st.scatter_chart(
            uploads[['rows', 'columns']].dropna(),
            x='rows', y='columns',
            use_container_width=True,
        )
st.markdown('</div>', unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Activity timeline
# ---------------------------------------------------------------------------

st.markdown('<div class="analytics-card">', unsafe_allow_html=True)
st.subheader('Daily Activity')

timeline = logs.groupby('date').size().reset_index(name='events')
if len(timeline) > 1:
    st.line_chart(timeline.set_index('date'), use_container_width=True)
else:
    st.caption('Need data from multiple days to show a timeline.')
st.markdown('</div>', unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Raw log explorer
# ---------------------------------------------------------------------------

st.markdown('<div class="analytics-card">', unsafe_allow_html=True)
with st.expander('🗂 Raw Event Log'):
    event_filter = st.multiselect('Filter by event', sorted(logs['event'].unique()), default=[])
    view = logs[logs['event'].isin(event_filter)] if event_filter else logs
    st.dataframe(
        view.sort_values('ts', ascending=False).head(500),
        use_container_width=True,
    )
    csv_bytes = view.to_csv(index=False).encode('utf-8')
    st.download_button('Export as CSV', csv_bytes, 'neatly_events.csv', 'text/csv')
st.markdown('</div>', unsafe_allow_html=True)
