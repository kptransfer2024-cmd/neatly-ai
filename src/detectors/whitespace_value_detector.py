"""Detects string cells that contain only whitespace — semantically null but not NaN."""
import pandas as pd

_MIN_WHITESPACE_COUNT = 1   # even one blank cell is worth surfacing


def detect(df: pd.DataFrame) -> list[dict]:
    """Return one issue dict per column containing whitespace-only cells."""
    if df.empty:
        return []

    string_cols = [c for c in df.columns if str(df[c].dtype) in ('object', 'str')]
    issues = []

    for col in string_cols:
        series = df[col].dropna()
        if len(series) == 0:
            continue

        ws_mask = series.astype(str).str.strip() == ''
        ws_count = int(ws_mask.sum())

        if ws_count < _MIN_WHITESPACE_COUNT:
            continue

        total_non_null = len(series)
        ws_pct = round(ws_count / total_non_null * 100, 2)
        severity = 'high' if ws_pct > 20 else 'medium' if ws_pct > 5 else 'low'
        row_indices = [int(i) for i in series.index[ws_mask].tolist()[:100]]

        issues.append({
            'detector': 'whitespace_value_detector',
            'type': 'whitespace_values',
            'columns': [col],
            'severity': severity,
            'row_indices': row_indices,
            'summary': '',
            'sample_data': {
                col: {
                    'whitespace_count': ws_count,
                    'whitespace_pct': ws_pct,
                    'total_non_null': total_non_null,
                }
            },
            'actions': [
                {
                    'id': 'null_out_whitespace',
                    'label': 'Null Out',
                    'description': f'Replace whitespace-only cells in "{col}" with NaN',
                    'params': {'column': col},
                },
                {
                    'id': 'drop_whitespace_rows',
                    'label': 'Drop Rows',
                    'description': f'Remove rows where "{col}" is whitespace-only',
                    'params': {'column': col},
                },
            ],
        })

    return issues
