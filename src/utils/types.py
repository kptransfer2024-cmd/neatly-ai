"""Shared type definitions for Neatly AI.

Use these TypedDicts for documentation, editor autocompletion, and as
a single source of truth for the issue dict schema.
"""
from typing import Any, TypedDict


class IssueDict(TypedDict, total=False):
    """Schema for a single data-quality issue returned by a detector.

    Required fields (always present after orchestrator normalization):
        detector, type, columns, severity, row_indices, summary, sample_data, actions

    Optional fields added by specific detectors:
        column, sub_type, dtype, suggested_dtype, lower_fence, upper_fence,
        outlier_count, missing_count, missing_pct, outlier_pct, ...
    """
    # Core (required after _normalize_issue)
    detector: str
    type: str
    columns: list[str]
    severity: str          # 'low' | 'medium' | 'high'
    row_indices: list[int]
    summary: str
    sample_data: dict[str, Any]
    actions: list[dict[str, Any]]

    # Singular column alias (some detectors; normalized by orchestrator)
    column: str

    # missing_value_detector extras
    missing_count: int
    missing_pct: float
    dtype: str

    # outlier_detector extras
    lower_fence: float
    upper_fence: float
    outlier_count: int
    outlier_pct: float
    min_val: float
    max_val: float

    # consistency_cleaner extras
    sub_type: str          # 'extra_whitespace' | 'mixed_case' | 'mixed_date_format'

    # schema_analyzer extras
    suggested_dtype: str

    # explanation layer
    explanation: str


class DiagnosisResult(TypedDict):
    """Return type of orchestrator.run_diagnosis()."""
    issues: list[IssueDict]
    quality_score: float
    column_profiles: dict[str, Any]
    column_contexts: list[Any]
    diagnosed_at: str          # ISO-8601 UTC string
    row_count: int
    column_count: int
    failed_detectors: list[str]


class CleaningLogEntry(TypedDict, total=False):
    """One entry appended to cleaning_log by transformation_executor."""
    action: str
    column: str
    # fill_missing
    strategy: str
    fill_value: Any
    filled_count: int
    # drop_*
    rows_dropped: int
    row_count_before: int
    row_count_after: int
    # clip_outliers
    lower_bound: float
    upper_bound: float
    clipped_count: int
    # cast_column
    target_dtype: str
    # normalize_text
    operation: str
    # drop_column
    columns_before: int
    columns_after: int
