"""Brief data-context summary for the upload preview.

Scans only column headers + dtypes (never row data) to produce a one-to-three
line markdown intro. Fully deterministic: no API, no tokens, O(n_columns).
"""
from collections import Counter

import pandas as pd

# Theme inference — small keyword sets tuned for common business datasets.
_THEMES: dict[str, frozenset[str]] = {
    'customer / contact': frozenset({
        'customer', 'client', 'user', 'email', 'phone', 'mobile', 'contact', 'member',
    }),
    'sales / orders': frozenset({
        'order', 'sale', 'transaction', 'purchase', 'invoice', 'cart', 'checkout',
    }),
    'financial': frozenset({
        'revenue', 'profit', 'cost', 'balance', 'credit', 'debit', 'payment', 'tax',
    }),
    'product / inventory': frozenset({
        'product', 'sku', 'inventory', 'stock', 'item', 'brand', 'supplier',
    }),
    'HR / employee': frozenset({
        'employee', 'salary', 'department', 'hire', 'manager', 'position',
    }),
    'events / analytics': frozenset({
        'event', 'session', 'click', 'page', 'url', 'device', 'browser', 'visit',
    }),
    'geographic': frozenset({
        'country', 'city', 'state', 'zip', 'postal', 'region',
        'latitude', 'longitude', 'address',
    }),
}

# Salient-column signals — fields a human would immediately look for.
_SALIENT: frozenset[str] = frozenset({
    'email', 'phone', 'date', 'time', 'timestamp', 'price', 'amount',
    'total', 'revenue', 'cost', 'id', 'name', 'status',
})

_THEME_MIN_MATCHES = 2  # require 2+ hits before claiming a theme


def summarize_data_context(df: pd.DataFrame, source_name: str | None = None) -> str:
    """Return a brief markdown intro describing the dataset.

    Args:
        df: the DataFrame to describe.
        source_name: file name or table name; rendered as the title if given.

    Returns:
        Markdown string with up to three lines: shape + dtype mix, inferred
        theme, and a short list of salient columns. Empty string if the frame
        is empty-shaped (no columns).
    """
    if df.shape[1] == 0:
        return ''

    n_rows, n_cols = df.shape
    dtype_counts = _count_dtypes(df)
    theme = _infer_theme(df.columns)
    highlights = _salient_columns(df.columns, limit=4)

    header_parts: list[str] = []
    if source_name:
        header_parts.append(f'**{source_name}**')
    header_parts.append(f'{n_rows:,} rows × {n_cols} columns')
    if dtype_counts:
        kinds = ', '.join(f'{cnt} {kind}' for kind, cnt in dtype_counts.most_common())
        header_parts.append(f'({kinds})')
    line1 = ' · '.join(header_parts)

    lines = [line1]
    if theme:
        lines.append(f'Looks like **{theme}** data.')
    if highlights:
        lines.append('Key columns: ' + ', '.join(f'`{c}`' for c in highlights) + '.')
    return '  \n'.join(lines)


def _count_dtypes(df: pd.DataFrame) -> Counter:
    counts: Counter = Counter()
    for dt in df.dtypes:
        if pd.api.types.is_bool_dtype(dt):
            counts['boolean'] += 1
        elif pd.api.types.is_numeric_dtype(dt):
            counts['numeric'] += 1
        elif pd.api.types.is_datetime64_any_dtype(dt):
            counts['date/time'] += 1
        else:
            counts['text'] += 1
    return counts


def _tokens(name: object) -> set[str]:
    s = str(name).lower().replace('-', '_').replace(' ', '_')
    return {t for t in s.split('_') if t}


def _infer_theme(columns) -> str | None:
    all_tokens: set[str] = set()
    for c in columns:
        all_tokens |= _tokens(c)
    best_theme, best_score = None, 0
    for theme, kws in _THEMES.items():
        score = len(all_tokens & kws)
        if score > best_score:
            best_theme, best_score = theme, score
    return best_theme if best_score >= _THEME_MIN_MATCHES else None


def _salient_columns(columns, limit: int = 4) -> list[str]:
    picked: list[str] = []
    for c in columns:
        if len(picked) >= limit:
            break
        if _tokens(c) & _SALIENT:
            picked.append(str(c))
    return picked
