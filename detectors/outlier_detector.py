"""Detects statistical outliers in numeric columns using IQR fencing."""
import pandas as pd


def detect(df: pd.DataFrame) -> list[dict]:
    """Return one issue dict per numeric column that has outliers.

    Issue dict shape:
        {
            'type': 'outliers',
            'column': str,
            'outlier_count': int,
            'lower_fence': float,
            'upper_fence': float,
            'min_val': float,
            'max_val': float,
        }
    """
    raise NotImplementedError
