"""Privacy guardrails: validate that no raw row data is passed to external services."""
from __future__ import annotations

import json
from typing import Any

_BLOCKED_KEYS = {"row_indices", "raw_rows", "sample_rows", "individual_records"}
_MAX_LIST_OF_DICTS = 500


def validate_no_raw_rows(payload: Any, context: str = "payload") -> None:
    """Raise ValueError if payload contains raw row data (blocked keys or large row lists).

    Call before every external API invocation that receives data derived from the DataFrame.
    """
    _check_blocked_keys(payload, context)
    _check_large_list_of_dicts(payload, context)


def _check_blocked_keys(obj: Any, context: str, depth: int = 0) -> None:
    """Recursively assert that no blocked key exists in any nested dict."""
    if depth > 12:
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in _BLOCKED_KEYS:
                raise ValueError(
                    f"[privacy_guard] '{context}' contains blocked key '{k}' — "
                    "raw row data must never be sent to external services."
                )
            _check_blocked_keys(v, context, depth + 1)
    elif isinstance(obj, list):
        for item in obj:
            _check_blocked_keys(item, context, depth + 1)


def _check_large_list_of_dicts(obj: Any, context: str, depth: int = 0) -> None:
    """Raise if a list of dicts exceeds row-count threshold — likely raw records."""
    if depth > 8:
        return
    if isinstance(obj, list):
        if len(obj) > _MAX_LIST_OF_DICTS and all(isinstance(x, dict) for x in obj[:10]):
            raise ValueError(
                f"[privacy_guard] '{context}' contains a list of {len(obj)} dicts — "
                "this likely represents raw row data. Only aggregate summaries are permitted."
            )
        for item in obj:
            _check_large_list_of_dicts(item, context, depth + 1)
    elif isinstance(obj, dict):
        for v in obj.values():
            _check_large_list_of_dicts(v, context, depth + 1)


def sanitize_for_logging(payload: Any) -> str:
    """Return a JSON-serializable string safe for log output (truncates large arrays)."""
    try:
        return json.dumps(_truncate(payload), default=str, indent=2)
    except Exception:
        return str(payload)[:500]


def _truncate(obj: Any, depth: int = 0) -> Any:
    """Recursively truncate large lists for safe display."""
    if depth > 6:
        return "..."
    if isinstance(obj, list):
        if len(obj) > 20:
            return obj[:20] + [f"... ({len(obj) - 20} more items)"]
        return [_truncate(x, depth + 1) for x in obj]
    if isinstance(obj, dict):
        return {k: _truncate(v, depth + 1) for k, v in obj.items()}
    return obj
