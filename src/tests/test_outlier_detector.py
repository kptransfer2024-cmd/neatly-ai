import pandas as pd
from detectors.outlier_detector import detect


def test_no_outliers():
    df = pd.DataFrame({'val': [10, 11, 12, 10, 11]})
    assert detect(df) == []


def test_detects_outlier():
    df = pd.DataFrame({'val': [10, 11, 12, 10, 1000]})
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['type'] == 'outliers'
    assert issues[0]['columns'] == ['val']
    assert issues[0]['outlier_count'] >= 1


def test_empty_dataframe_returns_empty():
    assert detect(pd.DataFrame()) == []


def test_no_numeric_columns_returns_empty():
    df = pd.DataFrame({'name': ['a', 'b', 'c', 'd']})
    assert detect(df) == []


def test_constant_column_skipped():
    # IQR = 0 → no meaningful fence → skip
    df = pd.DataFrame({'x': [5, 5, 5, 5, 5]})
    assert detect(df) == []


def test_small_column_skipped():
    # Fewer than 4 non-null values → quartiles unstable → skip
    df = pd.DataFrame({'x': [1, 2, 100]})
    assert detect(df) == []


def test_nan_not_flagged_as_outlier():
    df = pd.DataFrame({'x': [1.0, 2.0, 3.0, 4.0, None, 1000.0]})
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['outlier_count'] == 1  # only 1000, NaN excluded


def test_detects_both_tails():
    df = pd.DataFrame({'x': [-1000, 10, 11, 12, 13, 14, 1000]})
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['outlier_count'] == 2  # both low and high outliers


def test_integer_column():
    df = pd.DataFrame({'x': [1, 2, 3, 4, 5, 100]})
    issues = detect(df)
    assert len(issues) == 1
    assert isinstance(issues[0]['outlier_count'], int)


def test_ignores_non_numeric_columns():
    df = pd.DataFrame({
        'name': ['a', 'b', 'c', 'd', 'e', 'f'],
        'val': [10, 11, 12, 13, 14, 1000],
    })
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['columns'] == ['val']


def test_multiple_numeric_columns_with_outliers():
    df = pd.DataFrame({
        'a': [1, 2, 3, 4, 5, 100],
        'b': [10, 20, 30, 40, 50, 5000],
    })
    issues = detect(df)
    assert len(issues) == 2
    assert {i['columns'][0] for i in issues} == {'a', 'b'}


def test_sample_indices_capped_at_100():
    # 20 baseline + 6 outliers detected: cap is 100, so all 6 should be included
    df = pd.DataFrame({'x': [1] * 20 + [100, 200, 300, 400, 500, 600, 700]})
    issues = detect(df)
    assert issues[0]['outlier_count'] > 5  # more than 5 outliers exist
    assert len(issues[0]['row_indices']) == issues[0]['outlier_count']  # all outliers included (cap is 100)


def test_outlier_pct_correct():
    df = pd.DataFrame({'x': [1, 2, 3, 4, 5, 100]})
    issues = detect(df)
    assert issues[0]['outlier_pct'] == round(1 / 6 * 100, 2)


def test_fence_values_bracket_outlier():
    df = pd.DataFrame({'x': [10, 11, 12, 13, 14, 15, 16, 17, 1000]})
    issues = detect(df)
    assert issues[0]['upper_fence'] < 1000
    assert issues[0]['max_val'] == 1000.0
