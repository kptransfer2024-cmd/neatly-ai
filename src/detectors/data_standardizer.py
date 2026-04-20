"""Data standardization detector and formatter.

Identifies columns that would benefit from standardization and suggests
transformations to normalize formatting (phone numbers, addresses, dates, currency, etc).
"""
import re
import pandas as pd
from typing import Tuple

_PHONE_PATTERN = re.compile(r'[\d\s\-\(\)\+\.]+')
_US_ZIP_PATTERN = re.compile(r'\b\d{5}(?:-\d{4})?\b')
_ISO_DATE_PATTERN = re.compile(r'\d{4}-\d{2}-\d{2}')
_CURRENCY_PATTERN = re.compile(r'[$£€¥₹]?\s*[\d,]+\.?\d*')

_MIN_COLUMN_SAMPLES = 10  # Need at least 10 non-null values to analyze


def detect(df: pd.DataFrame) -> list[dict]:
    """Detect columns that would benefit from standardization.
    
    Args:
        df: DataFrame to scan for standardization opportunities
        
    Returns:
        List of standardization suggestion dicts
    """
    if df.empty:
        return []
    
    issues = []
    string_cols = [col for col in df.columns if df[col].dtype in ('object', 'string', 'str')]
    
    for col in string_cols:
        series = df[col]
        non_null = series.dropna()
        
        if len(non_null) < _MIN_COLUMN_SAMPLES:
            continue
        
        # Check for phone numbers
        std_type = _detect_phone_column(col, non_null)
        if std_type:
            issues.append(_build_standardization_issue(col, std_type, 'phone', non_null))
            continue
        
        # Check for US zip codes
        std_type = _detect_zip_column(col, non_null)
        if std_type:
            issues.append(_build_standardization_issue(col, std_type, 'zip_code', non_null))
            continue
        
        # Check for dates
        std_type = _detect_date_column(col, non_null)
        if std_type:
            issues.append(_build_standardization_issue(col, std_type, 'date', non_null))
            continue
        
        # Check for currency
        std_type = _detect_currency_column(col, non_null)
        if std_type:
            issues.append(_build_standardization_issue(col, std_type, 'currency', non_null))
            continue
    
    return issues


def _detect_phone_column(col: str, series: pd.Series) -> bool:
    """Check if column likely contains phone numbers."""
    col_lower = col.lower()
    if not any(hint in col_lower for hint in ['phone', 'tel', 'mobile', 'cell']):
        return False
    
    # Check if values look like phone numbers (digits + formatting chars)
    values_str = series.astype(str).str.strip()
    matches = values_str.str.match(_PHONE_PATTERN, na=False)
    match_rate = matches.sum() / len(values_str)
    
    return match_rate >= 0.60


def _detect_zip_column(col: str, series: pd.Series) -> bool:
    """Check if column likely contains US zip codes."""
    col_lower = col.lower()
    if not any(hint in col_lower for hint in ['zip', 'postal', 'zip_code']):
        return False
    
    values_str = series.astype(str).str.strip()
    matches = values_str.str.contains(_US_ZIP_PATTERN, na=False)
    match_rate = matches.sum() / len(values_str)
    
    return match_rate >= 0.60


def _detect_date_column(col: str, series: pd.Series) -> bool:
    """Check if column likely contains dates."""
    col_lower = col.lower()
    if not any(hint in col_lower for hint in ['date', 'time', 'created', 'updated', 'timestamp']):
        return False
    
    try:
        pd.to_datetime(series, errors='coerce')
        non_null_orig = series.notna().sum()
        converted = pd.to_datetime(series, errors='coerce').notna().sum()
        return converted / non_null_orig >= 0.60
    except:
        return False


def _detect_currency_column(col: str, series: pd.Series) -> bool:
    """Check if column likely contains currency values."""
    col_lower = col.lower()
    if not any(hint in col_lower for hint in ['price', 'cost', 'amount', 'salary', 'fee', 'total', 'value']):
        return False
    
    values_str = series.astype(str).str.strip()
    matches = values_str.str.contains(_CURRENCY_PATTERN, na=False)
    match_rate = matches.sum() / len(values_str)
    
    return match_rate >= 0.60


def _build_standardization_issue(col: str, suggests_std: bool, std_type: str, series: pd.Series) -> dict:
    """Build a standardization suggestion issue."""
    samples = series.astype(str).str.strip().head(3).tolist()
    
    std_labels = {
        'phone': 'Phone Number',
        'zip_code': 'US Zip Code',
        'date': 'Date',
        'currency': 'Currency Amount',
    }
    
    std_descriptions = {
        'phone': 'Standardize phone numbers to a consistent format (e.g., (XXX) XXX-XXXX)',
        'zip_code': 'Standardize US zip codes (e.g., XXXXX or XXXXX-XXXX)',
        'date': 'Convert dates to ISO format (YYYY-MM-DD) for consistency',
        'currency': 'Standardize currency values by removing symbols and normalizing decimals',
    }
    
    return {
        'detector': 'data_standardizer',
        'type': 'standardization_suggested',
        'columns': [col],
        'severity': 'low',
        'row_indices': [],
        'summary': f'Column "{col}" appears to contain {std_labels.get(std_type, std_type)} data that could be standardized',
        'sample_data': {
            col: {
                'standardization_type': std_type,
                'sample_values': samples,
                'suggested_format': _get_suggested_format(std_type),
            }
        },
        'actions': [
            {
                'id': 'standardize_format',
                'label': f'Standardize {std_labels.get(std_type, std_type)}',
                'description': std_descriptions.get(std_type, f'Standardize {std_type} format'),
                'params': {'column': col, 'standardization_type': std_type},
            }
        ],
    }


def _get_suggested_format(std_type: str) -> str:
    """Get the suggested format string for each standardization type."""
    formats = {
        'phone': '(XXX) XXX-XXXX',
        'zip_code': 'XXXXX or XXXXX-XXXX',
        'date': 'YYYY-MM-DD (ISO 8601)',
        'currency': '$X,XXX.XX',
    }
    return formats.get(std_type, 'Standard format')
