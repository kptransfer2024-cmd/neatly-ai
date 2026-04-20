"""Detects duplicate rows in a DataFrame and returns structured issue dicts."""
import pandas as pd


def detect(df: pd.DataFrame) -> list[dict]:
    """Return a single issue dict if duplicate rows exist, else empty list.

    Issue dict shape:
        {
            'type': 'duplicates',
            'duplicate_count': int,
            'total_rows': int,
            'duplicate_pct': float,
            'sample_indices': list[int],
        }
    """
    dup_mask = df.duplicated(keep='first')
    dup_count = int(dup_mask.sum())
    if dup_count == 0:
        return []
    total = len(df)
    return [{
        'type': 'duplicates',
        'duplicate_count': dup_count,
        'total_rows': total,
        'duplicate_pct': round(dup_count / total * 100, 2),
        'sample_indices': dup_mask[dup_mask].index.tolist()[:5],
    }]
