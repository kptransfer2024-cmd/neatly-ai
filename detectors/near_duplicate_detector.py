"""Detects near-duplicate rows using normalized string comparison and adjacent similarity scanning."""
import difflib
import pandas as pd

_SIMILARITY_THRESHOLD = 0.85   # SequenceMatcher ratio ≥ 0.85 → near-duplicate
_MAX_ROWS_TO_SCAN = 500        # cap O(n) scan; sample first 500 rows on large dfs
_MIN_STRING_LENGTH = 3         # strings < 3 chars too short for meaningful similarity
_MAX_CARDINALITY_RATIO = 0.95  # skip column if nearly all unique (UUID/ID-like)
_MIN_CARDINALITY_RATIO = 0.01  # skip column if nearly constant


def detect(df: pd.DataFrame) -> list[dict]:
    """Return one issue dict per cluster of near-duplicate rows."""
    if df.empty:
        return []

    candidate_cols = _select_candidate_columns(df)
    if not candidate_cols:
        return []

    issues = []
    for col in candidate_cols:
        series = df[col].dropna()
        if len(series) == 0:
            continue

        normalized = _normalize(series)
        short_mask = normalized.str.len() < _MIN_STRING_LENGTH
        non_short_mask = ~short_mask
        non_short_series = series[non_short_mask]
        non_short_normalized = normalized[non_short_mask]

        if len(non_short_series) < 2:
            continue

        capped_series = non_short_series.iloc[:_MAX_ROWS_TO_SCAN] if len(non_short_series) > _MAX_ROWS_TO_SCAN else non_short_series
        capped_normalized = non_short_normalized.iloc[:_MAX_ROWS_TO_SCAN] if len(non_short_normalized) > _MAX_ROWS_TO_SCAN else non_short_normalized

        clusters_relative = _find_clusters(capped_normalized)
        clusters_absolute = [[capped_series.index[i] for i in cluster] for cluster in clusters_relative]

        for cluster in clusters_absolute:
            if len(cluster) >= 2:
                issue = _build_issue(col, cluster, df)
                issues.append(issue)

    return issues


def _select_candidate_columns(df: pd.DataFrame) -> list[str]:
    """Return string columns with moderate cardinality and non-trivial lengths."""
    candidates = []
    for col in df.columns:
        if str(df[col].dtype) not in ('object', 'str'):
            continue

        non_null = df[col].dropna()
        if len(non_null) == 0:
            continue

        unique_count = int(non_null.nunique())
        cardinality_ratio = unique_count / len(non_null)

        if cardinality_ratio > _MAX_CARDINALITY_RATIO:
            continue
        if cardinality_ratio <= _MIN_CARDINALITY_RATIO:
            continue

        candidates.append(col)
    return candidates


def _normalize(series: pd.Series) -> pd.Series:
    """Lowercase, strip, collapse whitespace."""
    return series.astype(str).str.lower().str.strip().str.replace(r'\s+', ' ', regex=True)


def _find_clusters(normalized: pd.Series) -> list[list[int]]:
    """Sort → adjacent SequenceMatcher scan → return list of index clusters.

    Each cluster is a list of original integer row positions that are
    mutually near-duplicate to at least one other row in the cluster.
    """
    if len(normalized) < 2:
        return []

    sorted_list = sorted(enumerate(normalized.values), key=lambda x: x[1])
    clusters = []
    current_cluster = [sorted_list[0][0]]

    for i in range(1, len(sorted_list)):
        prev_pos, prev_str = sorted_list[i - 1]
        curr_pos, curr_str = sorted_list[i]

        ratio = difflib.SequenceMatcher(None, prev_str, curr_str).ratio()
        if ratio >= _SIMILARITY_THRESHOLD:
            current_cluster.append(curr_pos)
        else:
            if len(current_cluster) >= 2:
                clusters.append(sorted(current_cluster))
            current_cluster = [curr_pos]

    if len(current_cluster) >= 2:
        clusters.append(sorted(current_cluster))

    return clusters


def _build_issue(col: str, cluster: list[int], df: pd.DataFrame) -> dict:
    """Construct a full issue dict for one cluster of near-duplicate rows.

    cluster: list of index labels (positions in df) for rows in the cluster
    """
    cluster_size = len(cluster)
    total_rows = len(df)
    severity = _severity_for_cluster(cluster_size, total_rows)

    cluster_sample_values = df.loc[cluster[:5], col].tolist()

    return {
        'detector': 'near_duplicate_detector',
        'type': 'near_duplicates',
        'column': col,
        'severity': severity,
        'row_indices': cluster,
        'summary': '',
        'sample_data': {
            col: {
                'cluster_size': cluster_size,
                'similarity_threshold': _SIMILARITY_THRESHOLD,
                'sample_values': cluster_sample_values,
            }
        },
        'actions': [
            {
                'id': 'merge_near_duplicates',
                'label': 'Merge Cluster',
                'description': 'Keep the first occurrence and drop the remaining rows in this cluster',
                'params': {'column': col, 'row_indices': cluster},
            },
            {
                'id': 'flag_near_duplicates',
                'label': 'Flag Cluster',
                'description': 'Add a boolean column marking these rows as near-duplicate candidates',
                'params': {'column': col, 'row_indices': cluster},
            },
        ],
    }


def _severity_for_cluster(cluster_size: int, total_rows: int) -> str:
    """Return severity based on cluster size relative to total rows."""
    cluster_pct = cluster_size / total_rows * 100
    if cluster_size >= 10 or cluster_pct > 20:
        return 'high'
    elif cluster_size >= 5 or cluster_pct > 10:
        return 'medium'
    else:
        return 'low'
