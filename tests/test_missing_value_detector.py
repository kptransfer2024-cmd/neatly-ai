import pandas as pd
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
    assert issue['columns'] == ['score']
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


def test_empty_dataframe_returns_empty():
    df = pd.DataFrame({'a': pd.Series([], dtype=float)})
    assert detect_missing(df) == []


def test_symmetric_distribution_suggests_fill_mean():
    # skew([1,2,3,4,5]) ≈ 0 → symmetric → fill_mean
    series = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    issue = {'column': 'x', 'missing_count': 1, 'missing_pct': 20.0, 'dtype': 'float64', 'sample_values': []}
    assert suggest_strategy(issue, series=series) == 'fill_mean'


def test_skewed_distribution_suggests_fill_median():
    # right-skewed: outlier at 1000 → median is robust
    series = pd.Series([1.0, 1.0, 1.0, 1.0, 1000.0])
    issue = {'column': 'x', 'missing_count': 1, 'missing_pct': 20.0, 'dtype': 'float64', 'sample_values': []}
    assert suggest_strategy(issue, series=series) == 'fill_median'


def test_no_series_falls_back_to_fill_median():
    issue = {'column': 'x', 'missing_count': 1, 'missing_pct': 20.0, 'dtype': 'float64', 'sample_values': []}
    assert suggest_strategy(issue) == 'fill_median'
