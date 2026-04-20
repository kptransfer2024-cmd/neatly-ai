"""Calls Claude API to generate plain-English explanations for detected issues.

RULE: Only column-level stats and summaries are sent — never raw data rows.
"""
import anthropic


def explain_issues(issues: list[dict], df_stats: dict) -> list[dict]:
    """Attach a plain-English 'explanation' key to each issue dict.

    Args:
        issues: List of issue dicts from detectors.
        df_stats: Column-level statistics (counts, dtypes, value distributions) —
                  must not contain raw row data.

    Returns:
        The same issue list with an 'explanation' str added to each dict.
    """
    raise NotImplementedError


def _build_stats_summary(df_stats: dict) -> str:
    """Format df_stats into a compact prompt-safe string."""
    raise NotImplementedError
