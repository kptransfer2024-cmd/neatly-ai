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


def test_mixed_date_format_at_80_percent_threshold():
    """Verify date format detection works at the minimum 80% threshold."""
    df = pd.DataFrame({'date': [
        # 4 ISO format dates (80% of 5 values)
        '2024-01-15', '2024-02-16', '2024-03-17', '2024-04-18',
        # 1 slash format (still mixed)
        '15/01/2024',
    ]})
    issues = [i for i in detect(df) if i.get('sub_type') == 'mixed_date_format']
    # Should detect at exactly 80% threshold (4 of one format, 1 of another)
    assert len(issues) == 1
    assert issues[0]['columns'] == ['date']


def test_mixed_date_format_below_80_percent_threshold():
    """Verify mixed date format NOT detected when below 80% threshold."""
    df = pd.DataFrame({'date': [
        # 4 ISO format dates
        '2024-01-15', '2024-02-16', '2024-03-17', '2024-04-18',
        # 1 slash format date
        '15/01/2024',
        # 5 non-date values (text) - brings total matched to 5/10 = 50%
        'notadate1', 'notadate2', 'notadate3', 'notadate4', 'notadate5',
    ]})
    issues = [i for i in detect(df) if i.get('sub_type') == 'mixed_date_format']
    # Should NOT detect since only 50% of values match date formats (below 80% threshold)
    assert len(issues) == 0
