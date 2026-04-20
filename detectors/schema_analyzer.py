"""Infers column types and flags columns whose inferred type differs from the stored dtype."""
import warnings
import pandas as pd

_PARSE_THRESHOLD = 0.95
_SAMPLE_SIZE = 100            # head-sample size for early rejection on large columns
_SAMPLE_REJECT_THRESHOLD = 0.5  # sample parse rate below this → skip full parse
_BOOLEAN_TOKENS = frozenset({'yes', 'no', 'true', 'false', '1', '0', 'y', 'n', 't', 'f'})


def detect(df: pd.DataFrame) -> list[dict]:
    """Return one issue dict per text column whose values parse as a more specific type."""
    issues = []
    for col in df.columns:
        dtype = df[col].dtype
        if str(dtype) not in ('object', 'str'):
            continue

        non_null = df[col].dropna()
        if len(non_null) == 0:
            continue

        suggested = _infer_type(non_null)
        if suggested is None:
            continue

        current_dtype = str(dtype)
        issues.append({
            'detector': 'schema_analyzer',
            'type': 'type_mismatch',
            'columns': [col],
            'severity': 'medium',
            'row_indices': [],
            'summary': '',
            'current_dtype': current_dtype,
            'suggested_dtype': suggested,
            'sample_values': non_null.head(5).tolist(),
            'sample_data': {
                col: {
                    'current_dtype': current_dtype,
                    'suggested_dtype': suggested,
                },
            },
            'actions': [
                {
                    'id': 'cast_column',
                    'label': f'Cast to {suggested}',
                    'description': f'Convert "{col}" from {current_dtype} to {suggested}.',
                    'params': {'column': col, 'target_dtype': suggested},
                },
            ],
        })
    return issues


def _passes_parse(non_null: pd.Series, parser) -> bool:
    """Return True if parser accepts ≥ _PARSE_THRESHOLD of the column.

    On columns larger than _SAMPLE_SIZE, first probe the head sample and
    reject early if the sample is clearly bad — this avoids expensive
    full-column datetime parses on pure-text columns.
    """
    if len(non_null) > _SAMPLE_SIZE:
        sample = non_null.head(_SAMPLE_SIZE)
        if parser(sample, errors='coerce').notna().mean() < _SAMPLE_REJECT_THRESHOLD:
            return False
    return parser(non_null, errors='coerce').notna().mean() >= _PARSE_THRESHOLD


def _infer_type(non_null: pd.Series) -> str | None:
    """Return 'numeric', 'datetime', 'boolean', or None — ordered by parse cost."""
    if _passes_parse(non_null, pd.to_numeric):
        return 'numeric'

    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        if _passes_parse(non_null, pd.to_datetime):
            return 'datetime'

    normalized = non_null.astype(str).str.strip().str.lower()
    unique = set(normalized.unique())
    if unique and unique.issubset(_BOOLEAN_TOKENS):
        return 'boolean'

    return None
