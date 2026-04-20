"""Calls Claude API to generate plain-English explanations for detected issues.

RULE: Only column-level stats and summaries are sent — never raw data rows.
"""
import anthropic

_MODEL = 'claude-sonnet-4-6'
_MAX_TOKENS = 256
_SYSTEM_PROMPT = (
    "You are a data-quality assistant helping a non-technical user clean a CSV. "
    "Given a structured description of a single issue found by automated detectors, "
    "write a concise 1-2 sentence plain-English explanation of WHAT the issue is "
    "and WHY it matters for analysis. Do not suggest fixes — the UI offers action "
    "buttons. Do not invent statistics that are not in the input. Refer to columns "
    "by name only; do not echo raw cell values back."
)
# Keys that may carry raw data values or row pointers — stripped before prompting
_UNSAFE_KEYS = frozenset({
    'sample_values', 'example_values', 'sample_indices',
    'row_indices', 'rows', 'raw_rows',
})


def explain_issues(
    issues: list[dict],
    df_stats: dict,
    client: anthropic.Anthropic | None = None,
) -> list[dict]:
    """Attach a plain-English 'explanation' key to each issue dict.

    Args:
        issues: List of issue dicts from detectors.
        df_stats: Column-level statistics (counts, dtypes, value distributions) —
                  must not contain raw row data.
        client: Injectable Anthropic client (for testing); defaults to env key.

    Returns:
        The same issue list with an 'explanation' str added to each dict.
    """
    if not issues:
        return issues
    client = client or anthropic.Anthropic()
    stats_summary = _build_stats_summary(df_stats)
    for issue in issues:
        issue['explanation'] = _explain_one(issue, stats_summary, client)
    return issues


def _explain_one(issue: dict, stats_summary: str, client: anthropic.Anthropic) -> str:
    """Single-issue explanation call. Returns a fallback on API failure."""
    user_prompt = _build_issue_prompt(issue, stats_summary)
    try:
        response = client.messages.create(
            model=_MODEL,
            max_tokens=_MAX_TOKENS,
            system=_SYSTEM_PROMPT,
            messages=[{'role': 'user', 'content': user_prompt}],
        )
    except anthropic.APIError:
        return _fallback_explanation(issue)
    text = ''.join(b.text for b in response.content if b.type == 'text').strip()
    return text or _fallback_explanation(issue)


def _build_issue_prompt(issue: dict, stats_summary: str) -> str:
    """Render one issue as a compact, prompt-safe description (stats only)."""
    safe = {k: v for k, v in issue.items() if k not in _UNSAFE_KEYS}
    lines = [f"Issue type: {safe.pop('type', 'unknown')}"]
    if 'column' in safe:
        lines.append(f"Column: {safe.pop('column')}")
    for k, v in safe.items():
        lines.append(f"{k}: {v}")
    if stats_summary:
        lines.append("")
        lines.append("Dataset context:")
        lines.append(stats_summary)
    return "\n".join(lines)


def _build_stats_summary(df_stats: dict) -> str:
    """Format df_stats into a compact prompt-safe string (stats only, no rows)."""
    if not df_stats:
        return ""
    return "\n".join(f"- {k}: {v}" for k, v in df_stats.items())


def _fallback_explanation(issue: dict) -> str:
    issue_type = issue.get('type', 'issue')
    col = issue.get('column')
    if col:
        return f"Detected {issue_type} in column '{col}'."
    return f"Detected {issue_type}."
