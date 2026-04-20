"""Calls Claude API to generate plain-English explanations for detected issues.

RULE: Only column-level stats and summaries are sent — never raw data rows.
"""
import asyncio
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
_UNSAFE_KEYS = frozenset({
    'sample_values', 'example_values', 'sample_indices',
    'row_indices', 'rows', 'raw_rows',
})
_MAX_CONCURRENT = 5


def explain_issues(
    issues: list[dict],
    df_stats: dict,
    client: anthropic.Anthropic | None = None,
) -> list[dict]:
    """Attach a plain-English 'explanation' key to each issue dict.

    For production use, batches API calls asynchronously for 5-10x faster processing.
    For testing, accepts synchronous mocked clients.

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

    if client is None:
        try:
            client = anthropic.Anthropic()
        except Exception:
            # No API key / cannot construct → fall back to templates for every issue
            for issue in issues:
                issue['explanation'] = _fallback_explanation(issue)
            return issues

    stats_summary = _build_stats_summary(df_stats)

    if _is_async_client(client):
        explanations = asyncio.run(_explain_batch_async(issues, stats_summary, client))
    else:
        explanations = _explain_batch_sync(issues, stats_summary, client)

    for issue, explanation in zip(issues, explanations):
        issue['explanation'] = explanation
    return issues


def _is_async_client(client) -> bool:
    """Check if client is an AsyncAnthropic instance (not mocks or regular client)."""
    return type(client).__name__ == 'AsyncAnthropic'


def _explain_batch_sync(
    issues: list[dict],
    stats_summary: str,
    client: anthropic.Anthropic,
) -> list[str]:
    """Synchronous batch: for testing and mocked clients."""
    return [_explain_one_sync(issue, stats_summary, client) for issue in issues]


async def _explain_batch_async(
    issues: list[dict],
    stats_summary: str,
    client,
) -> list[str]:
    """Async batch with rate limiting for production."""
    sem = asyncio.Semaphore(_MAX_CONCURRENT)

    async def explain_one_with_sem(issue: dict) -> str:
        async with sem:
            return await _explain_one_async(issue, stats_summary, client)

    return await asyncio.gather(*[explain_one_with_sem(issue) for issue in issues])


def _explain_one_sync(
    issue: dict,
    stats_summary: str,
    client: anthropic.Anthropic,
) -> str:
    """Single-issue explanation call (synchronous).

    Falls back to a template string on any client error, including missing
    API key (TypeError in _validate_headers) and transport failures — so the
    app works even when ANTHROPIC_API_KEY is not configured.
    """
    user_prompt = _build_issue_prompt(issue, stats_summary)
    try:
        response = client.messages.create(
            model=_MODEL,
            max_tokens=_MAX_TOKENS,
            system=_SYSTEM_PROMPT,
            messages=[{'role': 'user', 'content': user_prompt}],
        )
    except Exception:
        return _fallback_explanation(issue)
    text = ''.join(b.text for b in response.content if b.type == 'text').strip()
    return text or _fallback_explanation(issue)


async def _explain_one_async(
    issue: dict,
    stats_summary: str,
    client,
) -> str:
    """Single-issue explanation call (async)."""
    user_prompt = _build_issue_prompt(issue, stats_summary)
    try:
        response = await client.messages.create(
            model=_MODEL,
            max_tokens=_MAX_TOKENS,
            system=_SYSTEM_PROMPT,
            messages=[{'role': 'user', 'content': user_prompt}],
        )
    except (anthropic.APIError, Exception):
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
