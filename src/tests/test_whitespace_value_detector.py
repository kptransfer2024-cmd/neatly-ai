import pandas as pd
import pytest
from detectors.whitespace_value_detector import detect


def test_empty_df():
    assert detect(pd.DataFrame()) == []


def test_no_string_columns():
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [4.0, 5.0, 6.0]})
    assert detect(df) == []


def test_no_whitespace_cells():
    df = pd.DataFrame({'name': ['Alice', 'Bob', 'Charlie']})
    assert detect(df) == []


def test_detects_spaces_only():
    df = pd.DataFrame({'name': ['Alice', '   ', 'Charlie']})
    result = detect(df)
    assert len(result) == 1
    assert result[0]['columns'][0] == 'name'
    assert result[0]['type'] == 'whitespace_values'


def test_detects_tab_only():
    df = pd.DataFrame({'notes': ['ok', '\t', 'fine']})
    result = detect(df)
    assert len(result) == 1
    assert result[0]['sample_data']['notes']['whitespace_count'] == 1


def test_does_not_flag_real_nulls():
    df = pd.DataFrame({'col': ['Alice', None, '   ']})
    result = detect(df)
    assert len(result) == 1
    assert result[0]['sample_data']['col']['whitespace_count'] == 1


def test_multiple_whitespace_cells():
    df = pd.DataFrame({'col': ['  ', '  ', 'good', '  ']})
    result = detect(df)
    assert result[0]['sample_data']['col']['whitespace_count'] == 3


def test_multiple_string_columns():
    df = pd.DataFrame({
        'a': ['Alice', '   ', 'Bob'],
        'b': ['x', 'y', '  '],
    })
    result = detect(df)
    cols = {r['columns'][0] for r in result}
    assert cols == {'a', 'b'}


def test_actions_present():
    df = pd.DataFrame({'x': ['hello', '   ']})
    result = detect(df)
    ids = [a['id'] for a in result[0]['actions']]
    assert 'null_out_whitespace' in ids
    assert 'drop_whitespace_rows' in ids


def test_severity_low_for_single_cell():
    df = pd.DataFrame({'col': ['a'] * 98 + ['   ', 'b']})
    result = detect(df)
    assert result[0]['severity'] == 'low'


def test_severity_high_for_many_cells():
    df = pd.DataFrame({'col': ['   '] * 25 + ['ok'] * 75})
    result = detect(df)
    assert result[0]['severity'] == 'high'


def test_row_indices_capped_at_100():
    df = pd.DataFrame({'col': ['   '] * 200 + ['ok'] * 50})
    result = detect(df)
    assert len(result[0]['row_indices']) <= 100


def test_all_null_column_no_issue():
    df = pd.DataFrame({'col': [None, None, None]})
    assert detect(df) == []


def test_constant_whitespace_column():
    df = pd.DataFrame({'col': ['   ', '   ', '   ']})
    result = detect(df)
    assert len(result) == 1
    assert result[0]['sample_data']['col']['whitespace_count'] == 3
