"""Code snippet generator for transparency and learning.

Provides annotated Python code showing:
1. How each detector identifies data quality issues
2. What pandas operations are applied during transforms
"""

DETECTION_SNIPPETS = {
    'missing_value': '''\
# Count null values in each column
null_counts = df.isnull().sum()

# Identify columns with missing values
cols_with_missing = null_counts[null_counts > 0].index

# For column '{col}': collect indices of rows with missing values
missing_rows = df[df['{col}'].isna()].index.tolist()

# Decide imputation strategy based on distribution shape
clean_values = df['{col}'].dropna()
skewness = abs(clean_values.skew())
if skewness <= 0.5:
    strategy = 'mean'  # symmetric distribution
else:
    strategy = 'median'  # skewed distribution''',

    'duplicates': '''\
# Identify rows that are exact duplicates of earlier rows
duplicate_mask = df.duplicated(keep='first')

# Count and collect indices of duplicate rows
duplicate_count = duplicate_mask.sum()
duplicate_indices = duplicate_mask[duplicate_mask].index.tolist()''',

    'outliers': '''\
# For numeric columns, use Tukey's IQR method
q1 = df['{col}'].quantile(0.25)  # 25th percentile
q3 = df['{col}'].quantile(0.75)  # 75th percentile
iqr = q3 - q1  # interquartile range

# Outliers are values beyond 1.5×IQR from quartiles
lower_fence = q1 - 1.5 * iqr
upper_fence = q3 + 1.5 * iqr

# Find rows that fall outside the fence
outlier_mask = (df['{col}'] < lower_fence) | (df['{col}'] > upper_fence)
outlier_indices = outlier_mask[outlier_mask].index.tolist()''',

    'type_mismatch': '''\
# Test if a text column can be parsed as the suspected type
sample_values = df['{col}'].dropna().head(100)
parse_rate = pd.to_numeric(sample_values, errors='coerce').notna().mean()

# If 95%+ of values parse successfully, type inference is reliable
if parse_rate >= 0.95:
    suggested_dtype = 'numeric'  # column should be float or int

# Similarly for datetime:
parse_rate = pd.to_datetime(sample_values, errors='coerce').notna().mean()
if parse_rate >= 0.95:
    suggested_dtype = 'datetime'  # column should be datetime64''',

    'extra_whitespace': '''\
# Detect leading, trailing, or internal multiple spaces
original = df['{col}'].astype(str)
stripped = original.str.strip()

# Find rows where stripping changes the value
has_leading_trailing = original != stripped

# Find rows with internal multiple spaces
has_internal_ws = original.str.contains(r'\\s{2,}', regex=True, na=False)

# Any of the above = extra whitespace detected
affected_mask = has_leading_trailing | has_internal_ws''',

    'mixed_case': '''\
# Group values by their lowercased form
lowered = df['{col}'].str.lower()

# Count how many different original-case variants exist per lowercased group
variants = df['{col}'].groupby(lowered).nunique()

# If any group has 2+ variants, it's mixed case (e.g., 'USA', 'usa', 'Usa')
mixed_case_groups = variants[variants > 1].index
affected_mask = lowered.isin(mixed_case_groups)''',

    'mixed_date_format': '''\
# Check for multiple date formats in the same column
import re

iso_pattern = r'^\\d{4}-\\d{2}-\\d{2}'        # YYYY-MM-DD
slash_pattern = r'^\\d{1,2}/\\d{1,2}/\\d{4}'  # M/D/YYYY or MM/DD/YYYY
dot_pattern = r'^\\d{1,2}\\.\\d{1,2}\\.\\d{4}'  # D.M.YYYY
dash_dmy_pattern = r'^\\d{1,2}-\\d{1,2}-\\d{4}'  # D-M-YYYY

values = df['{col}'].astype(str)
iso_count = values.str.match(iso_pattern).sum()
slash_count = values.str.match(slash_pattern).sum()
dot_count = values.str.match(dot_pattern).sum()
dash_count = values.str.match(dash_dmy_pattern).sum()

# If 2+ formats are present and together cover most values → mixed format
format_counts = [iso_count, slash_count, dot_count, dash_count]
if sum(c > 0 for c in format_counts) >= 2:
    total_formatted = sum(format_counts)
    if total_formatted / len(values) >= 0.8:
        issue_type = 'mixed_date_format'  # mixed formats detected''',

    'near_duplicates': '''\
# Find rows that are very similar but not exact duplicates
# (using string similarity, approximate string matching, or domain-specific rules)

# This detection is data-dependent and domain-specific.
# For example, check if lowercased and stripped versions are identical:
normalized = df['{col}'].astype(str).str.strip().str.lower()

# Collect groups of rows with identical normalized values
for normalized_value, group in df.groupby(normalized).groups.items():
    if len(group) > 1:
        # Multiple rows normalize to the same value → likely near-duplicates
        near_dup_indices.append(group.tolist())''',

    'pattern_mismatch': '''\
# Test values against a specific regex pattern
pattern = r'^[A-Z]{2}\\d{5}$'  # Example: two letters + five digits

# Find rows matching and not matching the pattern
matches = df['{col}'].astype(str).str.match(pattern, na=False)
non_matching_count = (~matches).sum()

# Rows that don't match are flagged or dropped
invalid_indices = (~matches)[~matches].index.tolist()''',

    'out_of_range': '''\
# Check if numeric values fall within expected bounds
lower_bound = 0.0
upper_bound = 100.0

# Find rows outside the valid range
out_of_range_mask = (df['{col}'] < lower_bound) | (df['{col}'] > upper_bound)
out_of_range_count = out_of_range_mask.sum()

# These values can be clipped to bounds or dropped
out_of_range_indices = out_of_range_mask[out_of_range_mask].index.tolist()''',

    'constant_column': '''\
# Check if a column has only one unique value
unique_count = df['{col}'].nunique()

# A constant column has no variance — contributes nothing to analysis
if unique_count == 1:
    issue_type = 'constant_column'
    # The single value (redundant information)
    single_value = df['{col}'].iloc[0]''',
}


def transform_code(label: str, log_entry: dict) -> str:
    """Generate pandas code for a transform operation.

    Args:
        label: Human-readable action label (e.g., 'Fill mean', 'Drop duplicates')
        log_entry: Dict from cleaning_log with 'action', 'column', 'params', etc.

    Returns:
        Python code snippet showing the pandas operation.
    """
    action = log_entry.get('action', '').lower()
    col = log_entry.get('column')
    params = log_entry.get('params', {})

    # fill_missing: mean/median/mode
    if action == 'fill_missing':
        strategy = params.get('fill_value') or 'mean'
        if strategy == 'mean':
            return f"df['{col}'] = df['{col}'].fillna(df['{col}'].mean())"
        elif strategy == 'median':
            return f"df['{col}'] = df['{col}'].fillna(df['{col}'].median())"
        elif strategy == 'mode':
            return f"df['{col}'] = df['{col}'].fillna(df['{col}'].mode().iloc[0])"
        else:
            return f"df['{col}'] = df['{col}'].fillna({repr(strategy)})"

    # drop_missing: remove rows with nulls in column
    if action == 'drop_missing':
        return f"df = df.dropna(subset=['{col}']).reset_index(drop=True)"

    # drop_duplicates: remove exact duplicate rows
    if action == 'drop_duplicates':
        return "df = df.drop_duplicates(keep='first').reset_index(drop=True)"

    # clip_outliers: constrain values to IQR fence
    if action == 'clip_outliers':
        lower = params.get('lower')
        upper = params.get('upper')
        if lower is not None and upper is not None:
            return f"df['{col}'] = df['{col}'].clip(lower={lower:.2f}, upper={upper:.2f})"

    # cast_column: convert to another dtype
    if action == 'cast_column':
        target = params.get('target_dtype', 'str').lower()
        if target in ('int', 'int64'):
            return f"df['{col}'] = pd.to_numeric(df['{col}'], errors='coerce').astype('Int64')"
        elif target in ('float', 'float64'):
            return f"df['{col}'] = pd.to_numeric(df['{col}'], errors='coerce').astype('float64')"
        elif target in ('datetime', 'datetime64'):
            return f"df['{col}'] = pd.to_datetime(df['{col}'], errors='coerce')"
        elif target == 'bool':
            return f"df['{col}'] = df['{col}'].astype('bool')"
        else:
            return f"df['{col}'] = df['{col}'].astype('{target}')"

    # normalize_text: case/whitespace normalization
    if action == 'normalize_text':
        norm_type = params.get('normalization_type', 'strip').lower()
        if norm_type == 'strip_whitespace':
            return f"df['{col}'] = df['{col}'].str.strip()"
        elif norm_type == 'lowercase':
            return f"df['{col}'] = df['{col}'].str.lower()"
        elif norm_type == 'uppercase':
            return f"df['{col}'] = df['{col}'].str.upper()"
        elif norm_type == 'titlecase':
            return f"df['{col}'] = df['{col}'].str.title()"

    # merge_near_duplicates: drop all but first in cluster
    if action == 'merge_near_duplicates':
        indices = params.get('row_indices', [])
        if indices:
            return f"df = df.drop(index={indices[1:]}).reset_index(drop=True)"

    # flag_near_duplicates: add boolean flag column
    if action == 'flag_near_duplicates':
        indices = params.get('row_indices', [])
        if indices:
            return f"df['_flagged_near_duplicate'] = False\ndf.loc[{indices}, '_flagged_near_duplicate'] = True"

    # flag_invalid_patterns: mark rows that don't match pattern
    if action == 'flag_invalid_patterns':
        pattern = params.get('pattern', '.*')
        return f"df['{col}'] = df['{col}'].where(df['{col}'].astype(str).str.match(r'{pattern}'), None)"

    # drop_invalid_rows: remove rows that don't match pattern
    if action == 'drop_invalid_rows':
        pattern = params.get('pattern', '.*')
        return f"mask = df['{col}'].astype(str).str.match(r'{pattern}')\ndf = df[mask].reset_index(drop=True)"

    # drop_out_of_range_rows: remove rows outside bounds
    if action == 'drop_out_of_range_rows':
        lo = params.get('lower_bound')
        hi = params.get('upper_bound')
        if lo is not None and hi is not None:
            return f"df = df[(df['{col}'] >= {lo}) & (df['{col}'] <= {hi})].reset_index(drop=True)"

    # drop_column: remove column entirely
    if action == 'drop_column':
        return f"df = df.drop(columns=['{col}'])"

    # Unknown action — provide fallback
    return f"# Action: {label}\ndf = df.copy()  # (code not available)"
