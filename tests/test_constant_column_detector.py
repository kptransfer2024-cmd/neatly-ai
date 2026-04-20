import pandas as pd

from detectors.constant_column_detector import detect


# --- empty / no-match cases ---

def test_empty_dataframe_returns_empty():
    assert detect(pd.DataFrame()) == []


def test_no_columns_returns_empty():
    assert detect(pd.DataFrame(index=[0, 1, 2])) == []


def test_no_constant_columns_returns_empty():
    df = pd.DataFrame({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})
    assert detect(df) == []


# --- detection ---

def test_single_value_numeric_column_detected():
    df = pd.DataFrame({'flag': [1, 1, 1, 1]})
    issues = detect(df)
    assert len(issues) == 1
    issue = issues[0]
    assert issue['type'] == 'constant_column'
    assert issue['columns'] == ['flag']
    assert issue['sub_type'] == 'single_value'
    assert issue['sample_data']['flag']['constant_value'] == 1


def test_single_value_text_column_detected():
    df = pd.DataFrame({'status': ['active', 'active', 'active']})
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['sub_type'] == 'single_value'
    assert issues[0]['sample_data']['status']['constant_value'] == 'active'


def test_all_null_column_detected_as_all_null():
    df = pd.DataFrame({'x': [None, None, None]})
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['sub_type'] == 'all_null'
    assert 'constant_value' not in issues[0]['sample_data']['x']


def test_column_with_one_value_plus_nulls_detected():
    # Column has only "x" and NaN → one distinct non-null → still constant
    df = pd.DataFrame({'c': ['x', None, 'x', None]})
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['sub_type'] == 'single_value'
    assert issues[0]['sample_data']['c']['constant_value'] == 'x'


def test_multiple_constant_columns_each_flagged():
    df = pd.DataFrame({
        'a': [1, 1, 1],
        'b': ['same', 'same', 'same'],
        'c': [1, 2, 3],
    })
    issues = detect(df)
    flagged = {i['columns'][0] for i in issues}
    assert flagged == {'a', 'b'}


def test_mixed_null_column_not_flagged():
    df = pd.DataFrame({'c': ['x', 'y', None]})  # 2 distinct values
    assert detect(df) == []


# --- issue shape ---

def test_issue_has_canonical_schema():
    df = pd.DataFrame({'flag': [1, 1, 1]})
    issue = detect(df)[0]
    required = {'detector', 'type', 'columns', 'severity', 'row_indices',
                'summary', 'sample_data', 'actions'}
    assert required.issubset(issue.keys())
    assert issue['detector'] == 'constant_column_detector'
    assert issue['severity'] == 'high'
    assert issue['row_indices'] == []
    assert issue['summary'] == ''


def test_action_is_drop_column_with_params():
    df = pd.DataFrame({'flag': [1, 1, 1]})
    issue = detect(df)[0]
    assert len(issue['actions']) == 1
    action = issue['actions'][0]
    assert action['id'] == 'drop_column'
    assert action['params'] == {'column': 'flag'}
    assert 'label' in action and 'description' in action


def test_sample_data_reports_counts():
    df = pd.DataFrame({'c': ['x', 'x', 'x', None, None]})
    issue = detect(df)[0]
    data = issue['sample_data']['c']
    assert data['non_null_count'] == 3
    assert data['total_rows'] == 5


def test_constant_value_is_json_friendly():
    import numpy as np
    df = pd.DataFrame({'n': np.array([5, 5, 5], dtype='int64')})
    issue = detect(df)[0]
    # numpy.int64 → Python int for JSON serialization
    assert issue['sample_data']['n']['constant_value'] == 5
    assert type(issue['sample_data']['n']['constant_value']) is int
