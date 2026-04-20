import pandas as pd
import pytest
from detectors.duplicate_column_detector import detect


def test_empty_df():
    assert detect(pd.DataFrame()) == []


def test_single_column():
    df = pd.DataFrame({'a': [1, 2, 3]})
    assert detect(df) == []


def test_no_duplicates():
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    assert detect(df) == []


def test_detects_identical_columns():
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [1, 2, 3]})
    result = detect(df)
    assert len(result) == 1
    assert result[0]['type'] == 'duplicate_column'
    assert result[0]['column'] == 'b'


def test_reports_original_column():
    df = pd.DataFrame({'original': [1, 2, 3], 'copy': [1, 2, 3]})
    result = detect(df)
    assert result[0]['sample_data']['copy']['duplicate_of'] == 'original'


def test_three_identical_columns():
    df = pd.DataFrame({'a': [1, 2], 'b': [1, 2], 'c': [1, 2]})
    result = detect(df)
    assert len(result) == 2
    reported_cols = {r['column'] for r in result}
    assert 'a' not in reported_cols  # 'a' is the first — kept


def test_does_not_flag_different_columns():
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [1, 2, 4]})
    assert detect(df) == []


def test_handles_nulls_correctly():
    df = pd.DataFrame({'a': [1, None, 3], 'b': [1, None, 3]})
    result = detect(df)
    assert len(result) == 1
    assert result[0]['column'] == 'b'


def test_null_mismatch_not_flagged():
    df = pd.DataFrame({'a': [1, None, 3], 'b': [1, 2, 3]})
    assert detect(df) == []


def test_string_columns():
    df = pd.DataFrame({'x': ['foo', 'bar'], 'y': ['foo', 'bar']})
    result = detect(df)
    assert len(result) == 1
    assert result[0]['column'] == 'y'


def test_mixed_type_columns_not_flagged():
    df = pd.DataFrame({'a': [1, 2, 3], 'b': ['1', '2', '3']})
    assert detect(df) == []


def test_action_present():
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [1, 2, 3]})
    result = detect(df)
    ids = [a['id'] for a in result[0]['actions']]
    assert 'drop_duplicate_column' in ids


def test_severity_is_medium():
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [1, 2, 3]})
    result = detect(df)
    assert result[0]['severity'] == 'medium'


def test_row_indices_empty():
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [1, 2, 3]})
    result = detect(df)
    assert result[0]['row_indices'] == []
