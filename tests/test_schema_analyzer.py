import pandas as pd
from detectors.schema_analyzer import detect


def test_no_type_issues_returns_empty():
    df = pd.DataFrame({'a': [1, 2, 3], 'b': ['apple', 'banana', 'cherry']})
    assert detect(df) == []


def test_numeric_stored_as_text():
    df = pd.DataFrame({'price': ['10', '20', '30', '40', '50']})
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['type'] == 'type_mismatch'
    assert issues[0]['column'] == 'price'
    assert issues[0]['suggested_dtype'] == 'numeric'


def test_float_strings_detected_as_numeric():
    df = pd.DataFrame({'x': ['1.5', '2.7', '3.14', '0.001']})
    issues = detect(df)
    assert issues[0]['suggested_dtype'] == 'numeric'


def test_datetime_stored_as_text():
    df = pd.DataFrame({'d': ['2024-01-15', '2024-02-15', '2024-03-15', '2024-04-15']})
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['suggested_dtype'] == 'datetime'


def test_boolean_yes_no():
    df = pd.DataFrame({'active': ['yes', 'no', 'yes', 'no', 'yes']})
    issues = detect(df)
    assert issues[0]['suggested_dtype'] == 'boolean'


def test_boolean_true_false():
    df = pd.DataFrame({'flag': ['true', 'false', 'true', 'false']})
    issues = detect(df)
    assert issues[0]['suggested_dtype'] == 'boolean'


def test_boolean_mixed_case_and_whitespace():
    df = pd.DataFrame({'flag': [' YES ', 'no', 'Yes', ' NO']})
    issues = detect(df)
    assert issues[0]['suggested_dtype'] == 'boolean'


def test_already_numeric_skipped():
    df = pd.DataFrame({'x': [1, 2, 3, 4, 5]})
    assert detect(df) == []


def test_already_datetime_skipped():
    df = pd.DataFrame({'d': pd.to_datetime(['2024-01-01', '2024-02-01', '2024-03-01'])})
    assert detect(df) == []


def test_mixed_text_no_suggestion():
    df = pd.DataFrame({'notes': ['hello', 'world', 'foo', 'bar', 'baz']})
    assert detect(df) == []


def test_empty_dataframe_returns_empty():
    assert detect(pd.DataFrame()) == []


def test_all_null_column_skipped():
    df = pd.DataFrame({'x': pd.Series([None, None, None], dtype='object')})
    assert detect(df) == []


def test_threshold_at_95_percent_flagged():
    # 19 numeric + 1 bad = 95% → flagged
    df = pd.DataFrame({'n': [str(i) for i in range(19)] + ['oops']})
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['suggested_dtype'] == 'numeric'


def test_below_threshold_not_flagged():
    # 10 numeric + 10 text = 50% → not flagged
    df = pd.DataFrame({'mixed': [str(i) for i in range(10)] + ['a'] * 10})
    assert detect(df) == []


def test_sample_values_capped_at_five():
    df = pd.DataFrame({'x': [str(i) for i in range(20)]})
    issues = detect(df)
    assert len(issues[0]['sample_values']) == 5


def test_sample_values_are_first_five_non_null():
    df = pd.DataFrame({'x': ['2024-01-01', '2024-02-01', '2024-03-01',
                             '2024-04-01', '2024-05-01', '2024-06-01']})
    issues = detect(df)
    assert issues[0]['sample_values'] == [
        '2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01', '2024-05-01'
    ]


def test_multiple_columns_different_types():
    df = pd.DataFrame({
        'n': ['1', '2', '3', '4'],
        'd': ['2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01'],
        'b': ['yes', 'no', 'yes', 'no'],
        'text': ['hello', 'world', 'foo', 'bar'],
    })
    issues = detect(df)
    by_col = {i['column']: i['suggested_dtype'] for i in issues}
    assert by_col == {'n': 'numeric', 'd': 'datetime', 'b': 'boolean'}


def test_zero_one_flagged_as_numeric_not_boolean():
    # "0" and "1" parse as both; numeric wins because it's checked first
    df = pd.DataFrame({'x': ['0', '1', '0', '1', '1']})
    issues = detect(df)
    assert issues[0]['suggested_dtype'] == 'numeric'


def test_issue_preserves_current_dtype():
    df = pd.DataFrame({'x': ['1', '2', '3', '4']})
    issues = detect(df)
    assert issues[0]['current_dtype'] in ('object', 'str')
