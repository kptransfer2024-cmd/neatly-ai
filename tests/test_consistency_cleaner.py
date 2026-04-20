import pandas as pd
from detectors.consistency_cleaner import detect


def test_empty_dataframe_returns_empty():
    assert detect(pd.DataFrame()) == []


def test_no_text_columns_returns_empty():
    df = pd.DataFrame({'x': [1, 2, 3], 'y': [1.1, 2.2, 3.3]})
    assert detect(df) == []


def test_all_null_column_skipped():
    df = pd.DataFrame({'x': pd.Series([None, None, None], dtype='object')})
    assert detect(df) == []


def test_clean_text_column_returns_empty():
    df = pd.DataFrame({'fruit': ['apple', 'banana', 'cherry']})
    assert detect(df) == []


def test_numeric_columns_ignored():
    df = pd.DataFrame({
        'n': [1, 2, 3],
        'name': ['  apple', 'banana', 'cherry'],
    })
    issues = detect(df)
    assert all(i['columns'][0] == 'name' for i in issues)


# --- mixed_case ---

def test_mixed_case_detected():
    df = pd.DataFrame({'fruit': ['apple', 'Apple', 'APPLE', 'banana']})
    issues = [i for i in detect(df) if i['sub_type'] == 'mixed_case']
    assert len(issues) == 1
    assert issues[0]['type'] == 'inconsistent_format'
    assert issues[0]['columns'][0] == 'fruit'
    assert set(issues[0]['example_values']) == {'apple', 'Apple', 'APPLE'}


def test_case_distinct_values_not_flagged():
    # All values unique even when lowercased → no collision
    df = pd.DataFrame({'x': ['apple', 'banana', 'cherry', 'date']})
    issues = [i for i in detect(df) if i['sub_type'] == 'mixed_case']
    assert issues == []


def test_mixed_case_only_colliding_values_in_examples():
    df = pd.DataFrame({'x': ['red', 'Red', 'blue', 'green']})
    issues = [i for i in detect(df) if i['sub_type'] == 'mixed_case']
    assert len(issues) == 1
    assert set(issues[0]['example_values']) == {'red', 'Red'}
    assert 'blue' not in issues[0]['example_values']


# --- extra_whitespace ---

def test_leading_whitespace_detected():
    df = pd.DataFrame({'x': [' apple', 'banana', 'cherry']})
    issues = [i for i in detect(df) if i['sub_type'] == 'extra_whitespace']
    assert len(issues) == 1
    assert ' apple' in issues[0]['example_values']


def test_trailing_whitespace_detected():
    df = pd.DataFrame({'x': ['apple  ', 'banana', 'cherry']})
    issues = [i for i in detect(df) if i['sub_type'] == 'extra_whitespace']
    assert len(issues) == 1


def test_internal_double_space_detected():
    df = pd.DataFrame({'x': ['New  York', 'Boston', 'Chicago']})
    issues = [i for i in detect(df) if i['sub_type'] == 'extra_whitespace']
    assert len(issues) == 1
    assert 'New  York' in issues[0]['example_values']


def test_no_whitespace_issues():
    df = pd.DataFrame({'x': ['apple', 'banana', 'cherry']})
    issues = [i for i in detect(df) if i['sub_type'] == 'extra_whitespace']
    assert issues == []


def test_whitespace_examples_capped_at_five():
    df = pd.DataFrame({'x': [f' val{i}' for i in range(10)]})
    issues = [i for i in detect(df) if i['sub_type'] == 'extra_whitespace']
    assert len(issues[0]['example_values']) == 5


# --- mixed_date_format ---

def test_mixed_date_format_detected():
    df = pd.DataFrame({'d': [
        '2024-01-15', '2024-02-15', '01/15/2024', '02/15/2024', '2024-03-15'
    ]})
    issues = [i for i in detect(df) if i['sub_type'] == 'mixed_date_format']
    assert len(issues) == 1
    assert issues[0]['columns'][0] == 'd'


def test_consistent_date_format_not_flagged():
    df = pd.DataFrame({'d': [
        '2024-01-15', '2024-02-15', '2024-03-15', '2024-04-15'
    ]})
    issues = [i for i in detect(df) if i['sub_type'] == 'mixed_date_format']
    assert issues == []


def test_mostly_text_column_not_flagged_as_mixed_dates():
    # Only 1 of 5 matches a date pattern → below threshold
    df = pd.DataFrame({'x': ['hello', 'world', 'foo', 'bar', '2024-01-15']})
    issues = [i for i in detect(df) if i['sub_type'] == 'mixed_date_format']
    assert issues == []


def test_mixed_dot_and_dash_formats():
    df = pd.DataFrame({'d': [
        '15.01.2024', '16.02.2024', '15-01-2024', '16-02-2024'
    ]})
    issues = [i for i in detect(df) if i['sub_type'] == 'mixed_date_format']
    assert len(issues) == 1


# --- combined / misc ---

def test_multiple_sub_types_on_same_column():
    df = pd.DataFrame({'x': [' Apple', 'apple ', 'APPLE', 'banana']})
    sub_types = {i['sub_type'] for i in detect(df)}
    assert 'mixed_case' in sub_types
    assert 'extra_whitespace' in sub_types


def test_multiple_columns_each_flagged_independently():
    df = pd.DataFrame({
        'a': ['apple', 'Apple', 'APPLE'],
        'b': [' hello', 'world', 'foo'],
        'c': ['clean', 'values', 'here'],
    })
    cols_flagged = {i['columns'][0] for i in detect(df)}
    assert cols_flagged == {'a', 'b'}


def test_issue_shape():
    df = pd.DataFrame({'x': ['apple', 'Apple']})
    issues = detect(df)
    assert len(issues) == 1
    issue = issues[0]
    required_keys = {
        'detector', 'type', 'columns', 'severity', 'row_indices',
        'summary', 'sample_data', 'actions', 'sub_type', 'example_values',
    }
    assert required_keys <= set(issue.keys())
    assert issue['detector'] == 'consistency_cleaner'
    assert issue['type'] == 'inconsistent_format'
    assert issue['columns'] == ['x']
    assert isinstance(issue['example_values'], list)
    assert isinstance(issue['actions'], list)


def test_nan_values_ignored():
    df = pd.DataFrame({'x': ['apple', 'Apple', None, None]})
    issues = detect(df)
    # Should still detect mixed_case on the non-null values; not crash on NaN
    assert any(i['sub_type'] == 'mixed_case' for i in issues)
