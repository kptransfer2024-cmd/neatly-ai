import pandas as pd
from src.detectors.utils import severity_from_pct, get_string_columns


def test_severity_from_pct_high():
    assert severity_from_pct(25.0) == 'high'


def test_severity_from_pct_medium():
    assert severity_from_pct(10.0) == 'medium'


def test_severity_from_pct_low():
    assert severity_from_pct(2.0) == 'low'


def test_severity_at_exact_boundary_high():
    # pct == high (20.0) → not > high, so 'medium'
    assert severity_from_pct(20.0) == 'medium'


def test_severity_at_exact_boundary_medium():
    # pct == medium (5.0) → not > medium, so 'low'
    assert severity_from_pct(5.0) == 'low'


def test_severity_custom_high_threshold():
    assert severity_from_pct(35.0, high=30.0) == 'high'


def test_severity_custom_medium_threshold():
    assert severity_from_pct(15.0, high=20.0, medium=10.0) == 'medium'


def test_severity_custom_all_thresholds():
    assert severity_from_pct(45.0, high=40.0, medium=20.0) == 'high'
    assert severity_from_pct(35.0, high=40.0, medium=20.0) == 'medium'
    assert severity_from_pct(15.0, high=40.0, medium=20.0) == 'low'


def test_get_string_columns_object_dtype():
    df = pd.DataFrame({'name': ['a', 'b'], 'value': [1, 2]})
    cols = get_string_columns(df)
    assert cols == ['name']


def test_get_string_columns_str_dtype():
    # Python 3.13 + pandas 2.x infers string columns as dtype='str'
    df = pd.DataFrame({'name': pd.Series(['a', 'b'], dtype='str'), 'value': [1, 2]})
    cols = get_string_columns(df)
    assert 'name' in cols
    assert 'value' not in cols


def test_get_string_columns_mixed_dtypes():
    df = pd.DataFrame({
        'city': ['Paris', 'London'],
        'age': [25, 30],
        'score': [88.5, 92.3],
    })
    cols = get_string_columns(df)
    assert cols == ['city']


def test_get_string_columns_numeric_excluded():
    df = pd.DataFrame({'int_col': [1, 2], 'float_col': [1.5, 2.5]})
    cols = get_string_columns(df)
    assert cols == []


def test_get_string_columns_empty_df():
    df = pd.DataFrame()
    cols = get_string_columns(df)
    assert cols == []


def test_get_string_columns_no_string_columns():
    df = pd.DataFrame({'a': [1, 2], 'b': [3.0, 4.0], 'c': [True, False]})
    cols = get_string_columns(df)
    assert cols == []


def test_get_string_columns_all_string_columns():
    df = pd.DataFrame({
        'col1': ['a', 'b'],
        'col2': ['x', 'y'],
        'col3': ['p', 'q'],
    })
    cols = get_string_columns(df)
    assert len(cols) == 3
    assert set(cols) == {'col1', 'col2', 'col3'}
