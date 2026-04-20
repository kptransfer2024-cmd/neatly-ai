import pandas as pd
import pytest
from detectors.missing_value_detector import detect_missing, suggest_strategy


def test_no_missing_returns_empty():
    df = pd.DataFrame({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})
    assert detect_missing(df) == []


def test_numeric_30pct_suggests_fill_median():
    # 1 null in 4 rows = 25% — dtype float64 → fill_median
    df = pd.DataFrame({'score': [1.0, None, 3.0, 4.0]})
    issues = detect_missing(df)
    assert len(issues) == 1
    issue = issues[0]
    assert issue['column'] == 'score'
    assert issue['missing_count'] == 1
    assert issue['missing_pct'] == 25.0
    assert 'dtype' in issue
    assert 'sample_values' in issue
    assert suggest_strategy(issue) == 'fill_median'


def test_60pct_missing_suggests_drop_column():
    # 3 nulls in 5 rows = 60% → drop_column
    df = pd.DataFrame({'age': [None, None, None, 10.0, 20.0]})
    issues = detect_missing(df)
    assert len(issues) == 1
    assert issues[0]['missing_pct'] == 60.0
    assert suggest_strategy(issues[0]) == 'drop_column'


def test_object_30pct_suggests_fill_mode():
    # 1 null in 4 rows = 25%, dtype object → fill_mode
    df = pd.DataFrame({'city': ['Paris', None, 'Lyon', 'Paris']})
    issues = detect_missing(df)
    assert len(issues) == 1
    issue = issues[0]
    assert issue['dtype'] in ('object', 'str')  # pandas 2.x infers str on Python 3.13+
    assert suggest_strategy(issue) == 'fill_mode'


def test_all_null_column():
    df = pd.DataFrame({'x': [None, None, None]})
    issues = detect_missing(df)
    assert len(issues) == 1
    issue = issues[0]
    assert issue['missing_pct'] == 100.0
    assert issue['sample_values'] == []
    assert suggest_strategy(issue) == 'drop_column'
