"""Detects duplicate rows in a DataFrame and returns structured issue dicts."""
import pandas as pd


def detect(df: pd.DataFrame) -> list[dict]:
    """Return a single issue dict if duplicate rows exist.

    Issue dict shape:
        {
            'type': 'duplicates',
            'duplicate_count': int,
            'total_rows': int,
        }
    """
    raise NotImplementedError
