"""PII (Personally Identifiable Information) detector."""
import re
import pandas as pd
from typing import Tuple

# Regex patterns for PII detection
_PII_PATTERNS = {
    'email': re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$'),
    'phone': re.compile(r'^[\+1\-\.\s]*(\(?\d{3}\)?[\-\.\s]?\d{3}[\-\.\s]?\d{4})$'),
    'ssn': re.compile(r'^\d{3}-\d{2}-\d{4}$'),
    'credit_card': re.compile(r'^\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{4}$'),
}

# Column name hints that suggest PII content
_PII_NAME_HINTS = frozenset({
    'name', 'full_name', 'firstname', 'first_name', 'lastname', 'last_name',
    'ssn', 'social_security', 'social_security_number', 'tax_id',
    'credit_card', 'card_number', 'cc_number', 'cardnumber',
    'phone', 'telephone', 'phone_number',
    'email', 'email_address',
})

_MIN_MATCH_RATE = 0.60  # Need 60% of non-null values to match pattern
_MIN_INVALID_FOR_REPORT = 1  # Need at least 1 PII value + ≥60% match rate to report


def detect(df: pd.DataFrame) -> list[dict]:
    """Detect PII (Personally Identifiable Information) in a DataFrame.

    Args:
        df: DataFrame to scan for PII

    Returns:
        List of issue dicts, one per PII-containing column
    """
    if df.empty:
        return []

    issues = []
    string_cols = _get_string_columns(df)

    for col in string_cols:
        series = df[col]

        # Pattern-based detection (high confidence)
        pii_type, match_rate = _detect_by_pattern(col, series)

        if pii_type:
            row_indices = _get_matching_row_indices(series, _PII_PATTERNS[pii_type], limit=10)
            if len(row_indices) >= _MIN_INVALID_FOR_REPORT:
                sample_values = _get_sample_values(series, _PII_PATTERNS[pii_type], count=3)
                issues.append(
                    _build_issue(
                        col,
                        pii_type,
                        match_rate,
                        row_indices,
                        sample_values,
                        detection_method='regex_pattern',
                    )
                )
            continue

        # Name heuristic (lower confidence)
        pii_type = _detect_by_name_hint(col)
        if pii_type:
            issues.append(
                _build_issue(
                    col,
                    pii_type,
                    0.0,  # No actual pattern match
                    [],  # No specific rows, heuristic-based
                    [],
                    detection_method='column_name_hint',
                )
            )

    return issues


def _get_string_columns(df: pd.DataFrame) -> list[str]:
    """Get list of string column names."""
    return [col for col in df.columns if df[col].dtype in ('object', 'string', 'str')]


def _detect_by_pattern(column: str, series: pd.Series) -> Tuple[str | None, float]:
    """Detect PII by regex pattern matching.

    Args:
        column: Column name (unused, for logging)
        series: Series to scan

    Returns:
        (pii_type, match_rate) or (None, 0.0) if no pattern matches >=60%
    """
    non_null = series.dropna()
    if len(non_null) == 0:
        return None, 0.0

    # Normalize: convert to string and strip whitespace
    values_str = non_null.astype(str).str.strip()

    best_type = None
    best_rate = 0.0

    # Test each pattern in priority order (SSN > credit_card > email > phone)
    for pii_type in ['ssn', 'credit_card', 'email', 'phone']:
        pattern = _PII_PATTERNS[pii_type]
        matches = values_str.str.match(pattern, na=False)
        match_count = matches.sum()
        match_rate = match_count / len(values_str)

        if match_rate >= _MIN_MATCH_RATE and match_rate > best_rate:
            best_type = pii_type
            best_rate = match_rate
            break  # Priority order: stop at first match >=60%

    return best_type, best_rate


def _detect_by_name_hint(column: str) -> str | None:
    """Detect PII by column name heuristics.

    Args:
        column: Column name

    Returns:
        pii_type or None
    """
    col_lower = column.lower()

    # Map name hints to PII types
    if any(hint in col_lower for hint in ['ssn', 'social_security', 'tax_id']):
        return 'ssn'
    if any(hint in col_lower for hint in ['credit_card', 'card_number', 'cc_number']):
        return 'credit_card'
    if any(hint in col_lower for hint in ['email', 'email_address']):
        return 'email'
    if any(hint in col_lower for hint in ['phone', 'telephone', 'phone_number']):
        return 'phone'
    if any(hint in col_lower for hint in ['name', 'firstname', 'lastname']):
        return 'name'

    return None


def _get_matching_row_indices(series: pd.Series, pattern: re.Pattern, limit: int = 10) -> list[int]:
    """Get indices of rows matching a regex pattern.

    Args:
        series: Series to scan
        pattern: Compiled regex pattern
        limit: Max number of indices to return

    Returns:
        List of matching row indices (int type)
    """
    non_null_mask = series.notna()
    non_null_series = series[non_null_mask]

    # Normalize: convert to string and strip whitespace
    values_str = non_null_series.astype(str).str.strip()

    # Find matches
    matches = values_str.str.match(pattern, na=False)
    matching_indices = non_null_series.index[matches].tolist()

    # Convert to int and cap at limit
    return [int(i) for i in matching_indices[:limit]]


def _get_sample_values(series: pd.Series, pattern: re.Pattern, count: int = 3) -> list[str]:
    """Get sample values matching a pattern.

    Args:
        series: Series to sample from
        pattern: Compiled regex pattern
        count: Number of samples to return

    Returns:
        List of sample values as strings
    """
    non_null = series.dropna()
    if len(non_null) == 0:
        return []

    values_str = non_null.astype(str).str.strip()
    matches = values_str.str.match(pattern, na=False)
    matching_values = values_str[matches].unique()

    return [str(v) for v in matching_values[:count]]


def _build_issue(
    column: str,
    pii_type: str,
    match_rate: float,
    row_indices: list[int],
    sample_values: list[str],
    detection_method: str,
) -> dict:
    """Build a PII issue dict.

    Args:
        column: Column name
        pii_type: Type of PII detected (email, phone, ssn, credit_card, name)
        match_rate: Percentage of values matching pattern (0-100)
        row_indices: Row indices containing PII
        sample_values: Sample PII values
        detection_method: 'regex_pattern' or 'column_name_hint'

    Returns:
        Issue dict matching standard schema
    """
    pii_type_labels = {
        'email': 'Email Address',
        'phone': 'Phone Number',
        'ssn': 'Social Security Number',
        'credit_card': 'Credit Card Number',
        'name': 'Personal Name',
    }

    label = pii_type_labels.get(pii_type, pii_type.replace('_', ' ').title())

    # Mask sample values for safety
    masked_samples = [_mask_sample_value(v, pii_type) for v in sample_values]

    return {
        'detector': 'pii_detector',
        'type': 'pii_detected',
        'columns': [column],
        'severity': 'high',
        'row_indices': row_indices,
        'summary': f'Column "{column}" contains {label} data',
        'sample_data': {
            column: {
                'pii_type': pii_type,
                'match_rate': match_rate * 100.0,
                'sample_values': masked_samples,
                'detection_method': detection_method,
            }
        },
        'actions': [
            {
                'id': 'mask_pii',
                'label': f'Mask {label}',
                'description': f'Replace {label} values with masked placeholders (e.g., ***@***.com for emails)',
                'params': {'column': column, 'pii_type': pii_type, 'mask_type': 'partial'},
            },
            {
                'id': 'drop_column',
                'label': 'Remove Column',
                'description': f'Delete the entire "{column}" column containing {label}',
                'params': {'column': column},
            },
        ],
    }


def _mask_sample_value(value: str, pii_type: str) -> str:
    """Mask a sample PII value for safe display in logs/UI.

    Args:
        value: The PII value
        pii_type: Type of PII

    Returns:
        Masked version of the value
    """
    if pii_type == 'email':
        # user@domain.com → u***@domain.com
        parts = value.split('@')
        if len(parts) == 2:
            username = parts[0]
            domain = parts[1]
            return f"{username[0]}***@{domain}"
        return '***@***.com'
    elif pii_type == 'phone':
        # (555) 123-4567 → (***) ***-4567
        last_4 = value[-4:] if len(value) >= 4 else '****'
        return f'(***) ***-{last_4}'
    elif pii_type == 'ssn':
        # 123-45-6789 → ***-**-6789
        return '***-**-' + value[-4:] if len(value) >= 4 else '***-**-****'
    elif pii_type == 'credit_card':
        # 1234-5678-9012-3456 → ****-****-****-3456
        last_4 = value[-4:] if len(value) >= 4 else '****'
        return f'****-****-****-{last_4}'
    elif pii_type == 'name':
        # John Doe → J*** D***
        parts = value.split()
        return ' '.join(f"{p[0]}***" if len(p) > 1 else p for p in parts)

    return '***'
