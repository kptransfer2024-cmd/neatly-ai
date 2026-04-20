import pandas as pd
from detectors.duplicate_detector import detect


def test_no_duplicates_returns_empty():
    df = pd.DataFrame({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})
    assert detect(df) == []


def test_one_duplicate_detected():
    df = pd.DataFrame({'a': [1, 2, 1], 'b': ['x', 'y', 'x']})
    issues = detect(df)
    assert len(issues) == 1
    issue = issues[0]
    assert issue['type'] == 'duplicates'
    assert issue['duplicate_count'] == 1
    assert issue['total_rows'] == 3
    assert issue['duplicate_pct'] == round(1 / 3 * 100, 2)


def test_multiple_duplicates():
    df = pd.DataFrame({'a': [1, 1, 1, 2], 'b': ['x', 'x', 'x', 'y']})
    issues = detect(df)
    assert issues[0]['duplicate_count'] == 2


def test_row_indices_capped_at_five():
    df = pd.DataFrame({'a': [1] * 10})
    issues = detect(df)
    assert len(issues[0]['row_indices']) <= 5


def test_all_duplicates():
    df = pd.DataFrame({'a': [7, 7, 7]})
    issues = detect(df)
    assert issues[0]['duplicate_count'] == 2
    assert issues[0]['duplicate_pct'] == round(2 / 3 * 100, 2)


def test_original_row_not_flagged():
    # keep='first': row 0 kept, row 1 flagged
    df = pd.DataFrame({'a': [5, 5]})
    issues = detect(df)
    assert 0 not in issues[0]['row_indices']
    assert 1 in issues[0]['row_indices']
