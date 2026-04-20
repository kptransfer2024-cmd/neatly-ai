"""Internal analytics dashboard for Neatly AI.

Access: /admin via Streamlit multipage navigation.
Password gate: set ADMIN_PASSWORD in Streamlit secrets to require a password.
If secret is not set, the page is open (fine for local dev / solo founder).
"""
import json
from collections import Counter
from datetime import datetime

import pandas as pd
import streamlit as st

# Import from parent package using sys.path hack that Streamlit multipage requires
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.analytics import load_logs

# ---------------------------------------------------------------------------
# Password gate
# ---------------------------------------------------------------------------

_PASSWORD = st.secrets.get('ADMIN_PASSWORD', None)

if _PASSWORD:
    pwd = st.text_input('Admin password', type='password')
    if pwd != _PASSWORD:
        st.warning('Enter the admin password to continue.')
        st.stop()

# ---------------------------------------------------------------------------
# Load & parse
# ---------------------------------------------------------------------------

st.set_page_config(page_title='Neatly Admin', page_icon='📊', layout='wide')
st.title('📊 Neatly Analytics')

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

st.subheader('Overview')
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric('Sessions', sessions)
c2.metric('File Uploads', len(uploads))
c3.metric('Decisions Made', len(decisions))
c4.metric('Issues Skipped', len(skips))
c5.metric('Completion Rate', f'{completion_rate}%')

# ---------------------------------------------------------------------------
# Conversion funnel
# ---------------------------------------------------------------------------

st.divider()
st.subheader('Conversion Funnel')

funnel_events = [
    ('session_started',      'Session started'),
    ('file_uploaded',        'File uploaded'),
    ('diagnosis_completed',  'Diagnosis completed'),
    ('decision_made',        'At least 1 action taken'),
    ('session_completed',    'Session completed'),
]

funnel_rows = []
for event, label in funnel_events:
    if event == 'decision_made':
        count = logs[logs['event'] == event]['session_id'].nunique()
    else:
        count = logs[logs['event'] == event]['session_id'].nunique()
    funnel_rows.append({'Step': label, 'Sessions': count})

funnel_df = pd.DataFrame(funnel_rows)
st.bar_chart(funnel_df.set_index('Step'), use_container_width=True)

# ---------------------------------------------------------------------------
# Issue types detected
# ---------------------------------------------------------------------------

st.divider()
st.subheader('Issue Types Detected')

detected = logs[logs['event'] == 'diagnosis_completed']
if not detected.empty and 'issue_types' in detected.columns:
    all_types = []
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
        st.bar_chart(type_counts, use_container_width=True)
    else:
        st.caption('No issue type data yet.')
else:
    st.caption('No diagnosis events logged yet.')

# ---------------------------------------------------------------------------
# Action preferences
# ---------------------------------------------------------------------------

st.divider()
st.subheader('Most Applied Actions')

if not decisions.empty and 'action' in decisions.columns:
    action_counts = decisions['action'].value_counts().head(10)
    st.bar_chart(action_counts, use_container_width=True)
else:
    st.caption('No decision events logged yet.')

# ---------------------------------------------------------------------------
# Drop-off analysis
# ---------------------------------------------------------------------------

st.divider()
st.subheader('Where Do Users Drop Off?')
st.caption('Last recorded event per session — sessions that stopped here.')

last_events = (
    logs.sort_values('ts')
    .groupby('session_id')['event']
    .last()
    .value_counts()
)
if not last_events.empty:
    st.bar_chart(last_events, use_container_width=True)

# ---------------------------------------------------------------------------
# Dataset profile
# ---------------------------------------------------------------------------

st.divider()
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

# ---------------------------------------------------------------------------
# Activity timeline
# ---------------------------------------------------------------------------

st.divider()
st.subheader('Daily Activity')

timeline = logs.groupby('date').size().reset_index(name='events')
if len(timeline) > 1:
    st.line_chart(timeline.set_index('date'), use_container_width=True)
else:
    st.caption('Need data from multiple days to show a timeline.')

# ---------------------------------------------------------------------------
# Raw log explorer
# ---------------------------------------------------------------------------

st.divider()
with st.expander('🗂 Raw Event Log'):
    event_filter = st.multiselect('Filter by event', sorted(logs['event'].unique()), default=[])
    view = logs[logs['event'].isin(event_filter)] if event_filter else logs
    st.dataframe(
        view.sort_values('ts', ascending=False).head(500),
        use_container_width=True,
    )
    csv_bytes = view.to_csv(index=False).encode('utf-8')
    st.download_button('Export as CSV', csv_bytes, 'neatly_events.csv', 'text/csv')
