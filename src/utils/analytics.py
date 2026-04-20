"""Event-based analytics logger for Neatly AI.

Design goals:
- Never crash the app — all writes are best-effort (silent fail).
- Never log raw data — metadata only (counts, types, column names).
- Works on Streamlit Cloud: writes to /tmp/neatly_logs.jsonl which
  persists across multiple user sessions within the same deployment.
- Works locally: same path falls back to cwd if /tmp unavailable.
"""
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import streamlit as st

# JSONL: one JSON object per line — safe for concurrent appends
_LOG_PATH = Path("/tmp/neatly_logs.jsonl")
_LOG_FALLBACK = Path("neatly_logs.jsonl")


def _log_path() -> Path:
    try:
        _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        return _LOG_PATH
    except OSError:
        return _LOG_FALLBACK


def init_session() -> None:
    """Create a new session ID and fire session_started — called once per browser session."""
    if 'session_id' in st.session_state:
        return  # already initialized
    st.session_state['session_id'] = uuid.uuid4().hex[:8]
    st.session_state['_session_start'] = datetime.now(timezone.utc).isoformat()
    log_event('session_started')


def log_event(event: str, **meta) -> None:
    """Append one event to the log file. Silently ignores all errors."""
    try:
        entry = {
            'session_id': st.session_state.get('session_id', 'anon'),
            'event': event,
            'ts': datetime.now(timezone.utc).isoformat(),
            **{k: v for k, v in meta.items() if v is not None},
        }
        path = _log_path()
        with path.open('a', encoding='utf-8') as f:
            f.write(json.dumps(entry) + '\n')
    except Exception:
        pass


def load_logs() -> list[dict]:
    """Read all log entries from the JSONL file. Returns [] on any error."""
    entries = []
    for path in (_LOG_PATH, _LOG_FALLBACK):
        if path.exists():
            try:
                with path.open(encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            entries.append(json.loads(line))
                break  # found and read the file, stop looking
            except Exception:
                pass
    return entries
