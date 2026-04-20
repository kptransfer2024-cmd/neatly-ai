"""Internal analytics dashboard for Neatly AI.

Access: /admin via Streamlit multipage navigation.

Password gate:
  - Set ADMIN_PASSWORD in Streamlit secrets (Manage app → Settings → Secrets)
    to require a password. Without the secret, the page is open (OK for
    local dev / solo founder).
  - Once authenticated in a browser session, credentials are cached in
    session_state so widget interactions don't re-prompt.
"""
import hmac
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

st.set_page_config(page_title='Neatly Admin', page_icon='📊', layout='wide')

# Import theme from parent app
init_state_module = __import__('utils.session_state', fromlist=['init_state'])
init_state_module.init_state()

# Apply theme CSS from parent
st.markdown("""
<style>
/* Theme Variables */
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

# Apply theme from session state
theme = st.session_state.get('theme', 'dark')
st.markdown(f"<script>document.documentElement.setAttribute('data-theme', '{theme}');</script>", unsafe_allow_html=True)

# Render theme toggle
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
    """Return True if the user is authenticated (or no password is required).

    If ADMIN_PASSWORD secret is not set, the page is open. Otherwise show a
    sign-in form and cache success in session_state for the session.
    """
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

    # Constant-time comparison — avoids leaking length via response time
    if hmac.compare_digest(pwd or '', expected):
        st.session_state['_admin_authed'] = True
        st.rerun()
    else:
        st.error('Incorrect password.')
    return False


if not _check_password():
    st.stop()

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
st.markdown('</div>', unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Issue types detected
# ---------------------------------------------------------------------------

st.markdown('<div class="analytics-card">', unsafe_allow_html=True)
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
st.markdown('</div>', unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Action preferences
# ---------------------------------------------------------------------------

st.markdown('<div class="analytics-card">', unsafe_allow_html=True)
st.subheader('Most Applied Actions')

if not decisions.empty and 'action' in decisions.columns:
    action_counts = decisions['action'].value_counts().head(10)
    st.bar_chart(action_counts, use_container_width=True)
else:
    st.caption('No decision events logged yet.')
st.markdown('</div>', unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Drop-off analysis
# ---------------------------------------------------------------------------

st.markdown('<div class="analytics-card">', unsafe_allow_html=True)
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
