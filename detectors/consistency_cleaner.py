"""Detects inconsistent formatting within columns (e.g. mixed case, extra whitespace, mixed date formats)."""
import pandas as pd


def detect(df: pd.DataFrame) -> list[dict]:
    """Return one issue dict per column with detected formatting inconsistencies.

    Issue dict shape:
        {
            'type': 'inconsistent_format',
            'column': str,
            'sub_type': str,   # e.g. 'mixed_case' | 'extra_whitespace' | 'mixed_date_format'
            'example_values': list,
        }
    """
    raise NotImplementedError
