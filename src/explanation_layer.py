"""Generates plain-English explanations for detected issues using static templates.

No API calls — zero latency, zero cost, fully deterministic.
"""

_TEMPLATES: dict[str, str] = {
    'missing_value':             "{col} is missing {missing_count} values ({missing_pct}% of rows) — fill or drop before analysis.",
    'outliers':                  "{col} has {outlier_count} statistical outliers ({outlier_pct}% of non-null values) beyond the IQR fence [{lower_fence}, {upper_fence}].",
    'duplicates':                "{duplicate_count} fully duplicate rows found — these inflate counts and skew aggregations.",
    'duplicate_column':          "{col} is an exact copy of '{duplicate_of}' — safe to drop.",
    'whitespace_values':         "{col} has {whitespace_count} whitespace-only cells ({whitespace_pct}% of non-null) — treat as missing.",
    'mixed_type':                "{col} mixes numeric and non-numeric values ({dirty_count} dirty, {dirty_pct}% of non-null) — coerce or drop.",
    'constant_column':           "{col} holds a single constant value across all rows — no information for analysis.",
    'inconsistent_format':       "{col} has inconsistent formatting — standardize before downstream use.",
    'type_mismatch':             "{col} has unexpected data types — check for parsing errors on upload.",
    'pattern_mismatch':          "{col} contains values that don't match the expected pattern.",
    'out_of_range':              "{col} has values outside the valid range.",
    'id_column':                 "{col} appears to be an ID column (high cardinality) — consider excluding from modeling.",
    'pii_detected':              "{col} may contain PII — mask or remove before sharing.",
    'near_duplicates':           "Near-duplicate rows detected — possible data entry errors or merged records.",
    'date_out_of_range':         "{col} contains dates outside the expected range — check for data entry errors.",
    'standardization_suggested': "{col} may benefit from standardization for consistent downstream processing.",
}


def explain_issues(issues: list[dict], df_stats: dict) -> list[dict]:  # noqa: ARG001
    """Attach a plain-English 'explanation' key to each issue dict.

    Uses static templates interpolated with real issue stats — no API calls.
    df_stats accepted for interface compatibility but not currently used.
    """
    for issue in issues:
        issue['explanation'] = _explain_one(issue)
    return issues


def _explain_one(issue: dict) -> str:
    """Generate a stat-interpolated explanation from a template."""
    issue_type = issue.get('type', '')
    template = _TEMPLATES.get(issue_type)
    if not template:
        return _fallback_explanation(issue)

    col = (issue.get('columns') or [None])[0] or issue.get('column', 'unknown column')
    ctx: dict = {'col': col, 'type': issue_type}

    for k, v in issue.items():
        if isinstance(v, (int, float, str)):
            ctx.setdefault(k, v)

    # Flatten sample_data for the primary column (picks up e.g. duplicate_of)
    sample = issue.get('sample_data', {})
    if isinstance(sample, dict) and col in sample and isinstance(sample[col], dict):
        for k, v in sample[col].items():
            if isinstance(v, (int, float, str)):
                ctx.setdefault(k, v)

    try:
        return template.format(**ctx)
    except KeyError:
        return _fallback_explanation(issue)


def _fallback_explanation(issue: dict) -> str:
    issue_type = issue.get('type', 'issue')
    col = (issue.get('columns') or [None])[0] or issue.get('column')
    if col:
        return f"Detected {issue_type} in column '{col}'."
    return f"Detected {issue_type}."
