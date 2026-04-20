import pandas as pd
import pytest
from detectors.mixed_type_detector import detect


def test_empty_df():
    assert detect(pd.DataFrame()) == []


def test_no_string_columns():
    df = pd.DataFrame({'a': [1, 2, 3]})
    assert detect(df) == []


def test_clean_numeric_strings():
    df = pd.DataFrame({'amount': ['100', '200', '300']})
    assert detect(df) == []  # all numeric — no dirty values


def test_clean_text_column():
    df = pd.DataFrame({'name': ['Alice', 'Bob', 'Charlie']})
    assert detect(df) == []  # non-numeric column, below threshold


def test_detects_mostly_numeric_with_dirty_values():
    # 8 numeric, 2 dirty — 80% numeric rate → should trigger
    df = pd.DataFrame({'revenue': ['100', '200', '300', '400', '500', '600', '700', '800', 'N/A', 'missing']})
    result = detect(df)
    assert len(result) == 1
    assert result[0]['type'] == 'mixed_type'
    assert result[0]['columns'][0] == 'revenue'


def test_does_not_flag_below_numeric_threshold():
    # 50% numeric — below the 60% threshold
    df = pd.DataFrame({'col': ['100', '200', '300', 'foo', 'bar', 'baz']})
    assert detect(df) == []


def test_does_not_flag_with_only_one_dirty_value():
    # Only 1 dirty value — below _MIN_DIRTY_COUNT = 2
    df = pd.DataFrame({'val': ['100', '200', '300', '400', 'N/A']})
    assert detect(df) == []


def test_sample_dirty_values_recorded():
    df = pd.DataFrame({'col': ['10', '20', '30', '40', '50', '60', '70', 'N/A', '--']})
    result = detect(df)
    assert len(result) == 1
    sample = result[0]['sample_data']['col']['sample_dirty_values']
    assert 'N/A' in sample or '--' in sample


def test_actions_present():
    df = pd.DataFrame({'col': ['10', '20', '30', '40', '50', '60', '70', 'N/A', '--']})
    result = detect(df)
    ids = [a['id'] for a in result[0]['actions']]
    assert 'coerce_to_numeric' in ids
    assert 'drop_non_numeric_rows' in ids


def test_null_values_ignored():
    df = pd.DataFrame({'col': ['10', '20', '30', None, '50', '60', '70', 'bad', 'val']})
    result = detect(df)
    assert len(result) == 1


def test_severity_set():
    df = pd.DataFrame({'revenue': ['100'] * 50 + ['N/A', 'missing', 'bad'] * 5})
    result = detect(df)
    assert result[0]['severity'] in ('low', 'medium', 'high')


def test_multiple_mixed_columns():
    df = pd.DataFrame({
        'a': ['1', '2', '3', '4', '5', '6', '7', '8', 'bad', 'val'],
        'b': ['10', '20', '30', '40', '50', '60', '70', '80', 'N/A', '--'],
    })
    result = detect(df)
    assert len(result) == 2


def test_row_indices_point_to_dirty_rows():
    df = pd.DataFrame({'col': ['10', '20', '30', '40', '50', '60', '70', 'N/A', '--']})
    result = detect(df)
    for idx in result[0]['row_indices']:
        assert not str(df['col'].iloc[idx]).replace('.', '', 1).lstrip('-').isdigit() or \
               df['col'].iloc[idx] in ('N/A', '--')
