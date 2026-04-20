"""Infers column types and flags columns whose inferred type differs from the stored dtype."""
import pandas as pd


def detect(df: pd.DataFrame) -> list[dict]:
    """Return one issue dict per column with a type mismatch or suspicious dtype.

    Issue dict shape:
        {
            'type': 'type_mismatch',
            'column': str,
            'current_dtype': str,
            'suggested_dtype': str,
            'sample_values': list,   # up to 5 representative values
        }
    """
    raise NotImplementedError
