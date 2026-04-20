"""Event-based analytics logger for Neatly AI.

Design goals:
- Never crash the app — all writes are best-effort (silent fail).
- Never log raw data — metadata only (counts, types, column names).
- Works on Streamlit Cloud with Supabase: when SUPABASE_URL + SUPABASE_KEY
  are set (env or Streamlit secrets), events are written to and read from
  the `neatly_events` Postgres table. Both the public app and admin app
  share the same table, so the admin dashboard sees real user data.
- Works locally without Supabase: falls back to /tmp/neatly_logs.jsonl.

Supabase table (run once in SQL Editor):
  CREATE TABLE neatly_events (
    id          BIGSERIAL PRIMARY KEY,
    session_id  TEXT        NOT NULL,
    event       TEXT        NOT NULL,
    ts          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    data        JSONB       NOT NULL DEFAULT '{}'
  );
  -- allow anon reads/writes (analytics only, no user PII)
  ALTER TABLE neatly_events ENABLE ROW LEVEL SECURITY;
  CREATE POLICY "anon_all" ON neatly_events FOR ALL TO anon USING (true) WITH CHECK (true);
"""
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import streamlit as st

_LOG_PATH = Path("/tmp/neatly_logs.jsonl")
_LOG_FALLBACK = Path("neatly_logs.jsonl")

# Module-level Supabase client — initialised once per process
_supabase_client = None
_supabase_checked = False


def _log_path() -> Path:
    try:
        _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        return _LOG_PATH
    except OSError:
        return _LOG_FALLBACK


def _get_supabase():
    """Return cached Supabase client, or None if not configured."""
    global _supabase_client, _supabase_checked
    if _supabase_checked:
        return _supabase_client
    _supabase_checked = True
    try:
        url = os.environ.get('SUPABASE_URL')
        key = os.environ.get('SUPABASE_KEY')
        if not url or not key:
            try:
                url = st.secrets.get('SUPABASE_URL')
                key = st.secrets.get('SUPABASE_KEY')
            except Exception:
                pass
        if url and key:
            from supabase import create_client
            _supabase_client = create_client(url, key)
    except Exception:
        pass
    return _supabase_client


def init_session() -> None:
    """Create a new session ID and fire session_started — called once per browser session."""
    if 'session_id' in st.session_state:
        return
    st.session_state['session_id'] = uuid.uuid4().hex[:8]
    st.session_state['_session_start'] = datetime.now(timezone.utc).isoformat()
    log_event('session_started')


def log_event(event: str, **meta) -> None:
    """Append one event to Supabase (when configured) and local JSONL fallback."""
    try:
        entry = {
            'session_id': st.session_state.get('session_id', 'anon'),
            'event': event,
            'ts': datetime.now(timezone.utc).isoformat(),
            **{k: v for k, v in meta.items() if v is not None},
        }

        # --- local JSONL (always written, fast, works offline) ---
        try:
            path = _log_path()
            with path.open('a', encoding='utf-8') as f:
                f.write(json.dumps(entry) + '\n')
        except Exception:
            pass

        # --- Supabase (written when configured) ---
        client = _get_supabase()
        if client:
            data_payload = {k: v for k, v in entry.items()
                            if k not in ('session_id', 'event', 'ts')}
            client.table('neatly_events').insert({
                'session_id': entry['session_id'],
                'event':      entry['event'],
                'ts':         entry['ts'],
                'data':       data_payload,
            }).execute()
    except Exception:
        pass


def load_logs() -> list[dict]:
    """Return all log entries. Reads from Supabase when configured, else local JSONL."""

    # --- Supabase ---
    try:
        client = _get_supabase()
        if client:
            resp = (
                client.table('neatly_events')
                .select('session_id, event, ts, data')
                .order('ts')
                .limit(10_000)
                .execute()
            )
            entries = []
            for row in resp.data:
                flat = {
                    'session_id': row['session_id'],
                    'event':      row['event'],
                    'ts':         row['ts'],
                }
                flat.update(row.get('data') or {})
                entries.append(flat)
            return entries
    except Exception:
        pass

    # --- local JSONL fallback ---
    entries = []
    for path in (_LOG_PATH, _LOG_FALLBACK):
        if path.exists():
            try:
                with path.open(encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            entries.append(json.loads(line))
                break
            except Exception:
                pass
    return entries
