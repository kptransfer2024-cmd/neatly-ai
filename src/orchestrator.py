"""Wires detectors → explanation_layer → returns diagnosis result.

Pure function: takes DataFrame, returns DiagnosisResult dict.
"""
import logging
import traceback
from datetime import datetime, timezone
from typing import Any
import pandas as pd

logger = logging.getLogger(__name__)

from detectors.missing_value_detector import detect_missing
from detectors.duplicate_detector import detect as detect_duplicates
from detectors.schema_analyzer import detect as detect_schema
from detectors.consistency_cleaner import detect as detect_consistency
from detectors.outlier_detector import detect as detect_outliers
from detectors.pattern_validator import detect as detect_patterns
from detectors.range_validator import detect as detect_ranges
from detectors.near_duplicate_detector import detect as detect_near_duplicates
from detectors.constant_column_detector import detect as detect_constant_columns
from detectors.whitespace_value_detector import detect as detect_whitespace_values
from detectors.mixed_type_detector import detect as detect_mixed_types
from detectors.duplicate_column_detector import detect as detect_duplicate_columns
from detectors.id_column_detector import detect as detect_id_columns
from explanation_layer import explain_issues
from context_interpreter import build_column_contexts

_REQUIRED_ISSUE_FIELDS = {
    'detector': '',
    'type': '',
    'columns': list,
    'severity': 'medium',
    'row_indices': list,
    'summary': '',
    'sample_data': dict,
    'actions': list,
}


def run_diagnosis(df: pd.DataFrame) -> dict[str, Any]:
    """Run all detectors and attach explanations.

    Returns a DiagnosisResult dict with keys:
      - issues: list of issue dicts with explanations
      - quality_score: float 0.0–100.0
      - column_profiles: dict of schema info
      - diagnosed_at: datetime ISO string
      - row_count: int
      - column_count: int
    """
    issues = []
    failed_detectors: list[str] = []

    for detector_fn, detector_name, issue_type in [
        (detect_missing, 'missing_value_detector', 'missing_value'),
        (detect_duplicates, 'duplicate_detector', 'duplicates'),
        (detect_schema, 'schema_analyzer', 'type_mismatch'),
        (detect_consistency, 'consistency_cleaner', 'inconsistent_format'),
        (detect_outliers, 'outlier_detector', 'outliers'),
        (detect_patterns, 'pattern_validator', 'pattern_mismatch'),
        (detect_ranges, 'range_validator', 'out_of_range'),
        (detect_near_duplicates, 'near_duplicate_detector', 'near_duplicates'),
        (detect_constant_columns, 'constant_column_detector', 'constant_column'),
        (detect_whitespace_values, 'whitespace_value_detector', 'whitespace_values'),
        (detect_mixed_types, 'mixed_type_detector', 'mixed_type'),
        (detect_duplicate_columns, 'duplicate_column_detector', 'duplicate_column'),
        (detect_id_columns, 'id_column_detector', 'id_column'),
    ]:
        try:
            detector_issues = detector_fn(df)
            for issue in detector_issues or []:
                _normalize_issue(issue, detector_name, issue_type)
                issues.append(issue)
        except Exception as e:
            logger.error("[orchestrator] %s failed: %s\n%s", detector_name, e, traceback.format_exc())
            failed_detectors.append(detector_name)

    df_stats = _collect_df_stats(df)
    explained_issues = explain_issues(issues, df_stats)

    for issue in explained_issues:
        issue['summary'] = issue.pop('explanation', issue.get('summary', ''))

    quality_score = _compute_quality_score(issues, df)
    column_profiles = _extract_column_profiles(df_stats)

    try:
        column_contexts = build_column_contexts(df)
    except Exception as e:
        print(f"[orchestrator] context_interpreter failed: {e}\n{traceback.format_exc()}")
        column_contexts = []

    return {
        'issues': explained_issues,
        'quality_score': quality_score,
        'column_profiles': column_profiles,
        'column_contexts': column_contexts,
        'diagnosed_at': datetime.now(timezone.utc).isoformat(),
        'row_count': len(df),
        'column_count': len(df.columns),
    }


def _normalize_issue(issue: dict, detector_name: str, issue_type: str) -> None:
    """Backfill any missing schema fields so app.py never KeyErrors on an issue."""
    issue.setdefault('detector', detector_name)
    issue.setdefault('type', issue_type)
    for field, default in _REQUIRED_ISSUE_FIELDS.items():
        if field in issue:
            continue
        issue[field] = default() if isinstance(default, type) else default


def _collect_df_stats(df: pd.DataFrame) -> dict:
    """Build column-level stats safe to pass to the explanation layer (no raw rows)."""
    if df.empty:
        return {'rows': 0, 'columns': 0}

    stats = {
        'rows': len(df),
        'columns': len(df.columns),
    }

    for col in df.columns:
        series = df[col]
        col_stats = {
            'dtype': str(series.dtype),
            'non_null_count': int(series.notna().sum()),
            'null_count': int(series.isna().sum()),
        }

        if series.dtype in ('float64', 'int64', 'Int64'):
            numeric_vals = pd.to_numeric(series, errors='coerce')
            if numeric_vals.notna().any():
                col_stats['mean'] = float(numeric_vals.mean())
                col_stats['median'] = float(numeric_vals.median())
                col_stats['min'] = float(numeric_vals.min())
                col_stats['max'] = float(numeric_vals.max())
        elif series.dtype in ('object', 'str'):
            try:
                mode_val = series.mode()
                if len(mode_val) > 0:
                    col_stats['mode'] = str(mode_val.iloc[0])
            except (ValueError, IndexError):
                pass

        stats[f'{col}_stats'] = col_stats

    return stats


def _compute_quality_score(issues: list[dict], df: pd.DataFrame) -> float:
    """Compute data quality score 0–100 based on issue count and severity.

    High severity issues reduce score more than low severity.
    """
    if df.empty:
        return 0.0

    high_severity = sum(1 for i in issues if i.get('severity') == 'high')
    medium_severity = sum(1 for i in issues if i.get('severity') == 'medium')
    low_severity = sum(1 for i in issues if i.get('severity') == 'low')

    deduction = high_severity * 15 + medium_severity * 8 + low_severity * 3
    return max(0.0, min(100.0, 100.0 - deduction))


def _extract_column_profiles(df_stats: dict) -> dict[str, Any]:
    """Extract per-column dtype and null info from df_stats."""
    profiles = {}
    for key in df_stats:
        if key.endswith('_stats') and key != 'rows' and key != 'columns':
            col_name = key.replace('_stats', '')
            profiles[col_name] = df_stats[key]
    return profiles
