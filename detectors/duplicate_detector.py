"""Detects duplicate rows in a DataFrame and returns structured issue dicts."""
import pandas as pd

_SEVERITY_HIGH_PCT = 20.0
_SEVERITY_MED_PCT = 5.0


def detect(df: pd.DataFrame) -> list[dict]:
    """Return a single issue dict if duplicate rows exist, else empty list."""
    dup_mask = df.duplicated(keep='first')
    dup_count = int(dup_mask.sum())
    if dup_count == 0:
        return []
    total = len(df)
    dup_pct = round(dup_count / total * 100, 2)
    row_indices = dup_mask.to_numpy().nonzero()[0][:5].tolist()

    if dup_pct > _SEVERITY_HIGH_PCT:
        severity = 'high'
    elif dup_pct > _SEVERITY_MED_PCT:
        severity = 'medium'
    else:
        severity = 'low'

    return [{
        'detector': 'duplicate_detector',
        'type': 'duplicates',
        'columns': [],  # row-level issue — affects all columns equally
        'severity': severity,
        'row_indices': row_indices,
        'summary': '',
        'duplicate_count': dup_count,
        'total_rows': total,
        'duplicate_pct': dup_pct,
        'sample_data': {
            'duplicate_count': dup_count,
            'duplicate_pct': dup_pct,
            'total_rows': total,
        },
        'actions': [
            {
                'id': 'drop_duplicates',
                'label': 'Remove Duplicates',
                'description': f'Drop {dup_count} duplicate row(s), keeping the first occurrence.',
                'params': {},
            },
        ],
    }]
