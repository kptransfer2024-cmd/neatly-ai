import pandas as pd
import pytest
import streamlit as st

# Bootstrap minimal session_state before importing the module
if 'cleaning_log' not in st.session_state:
    st.session_state['cleaning_log'] = []

from transformation_executor import (
    drop_duplicates,
    fill_missing,
    drop_missing,
    clip_outliers,
    cast_column,
    normalize_text,
)


@pytest.fixture(autouse=True)
def clear_log():
    st.session_state['cleaning_log'] = []
    yield


# drop_duplicates tests
def test_drop_duplicates_removes_rows():
    df = pd.DataFrame({'a': [1, 1, 2]})
    result = drop_duplicates(df)
    assert len(result) == 2


def test_drop_duplicates_logs():
    df = pd.DataFrame({'a': [1, 1, 2]})
    drop_duplicates(df)
    assert len(st.session_state['cleaning_log']) == 1


def test_drop_duplicates_no_dupes():
    df = pd.DataFrame({'a': [1, 2, 3]})
    result = drop_duplicates(df)
    assert len(result) == 3
    assert len(st.session_state['cleaning_log']) == 0


# fill_missing tests
def test_fill_missing_mean():
    df = pd.DataFrame({'a': [1.0, None, 3.0]})
    result = fill_missing(df, 'a', 'mean')
    assert result['a'].isna().sum() == 0


def test_fill_missing_logs():
    df = pd.DataFrame({'a': [1.0, None, 3.0]})
    fill_missing(df, 'a', 'mean')
    assert len(st.session_state['cleaning_log']) == 1
    assert st.session_state['cleaning_log'][0]['action'] == 'fill_missing'
    assert st.session_state['cleaning_log'][0]['column'] == 'a'
    assert st.session_state['cleaning_log'][0]['filled_count'] == 1


def test_fill_missing_median():
    df = pd.DataFrame({'a': [1.0, None, 100.0]})
    result = fill_missing(df, 'a', 'median')
    assert result['a'].isna().sum() == 0


def test_fill_missing_mode():
    df = pd.DataFrame({'a': ['x', 'x', None, 'y']})
    result = fill_missing(df, 'a', 'mode')
    assert result['a'].isna().sum() == 0


def test_fill_missing_constant():
    df = pd.DataFrame({'a': [1.0, None, 3.0]})
    result = fill_missing(df, 'a', 'constant', fill_value=999)
    assert result['a'].isna().sum() == 0
    assert result['a'].iloc[1] == 999


def test_fill_missing_invalid_strategy():
    df = pd.DataFrame({'a': [1.0, None, 3.0]})
    with pytest.raises(ValueError):
        fill_missing(df, 'a', 'invalid')


def test_fill_missing_no_nulls():
    df = pd.DataFrame({'a': [1.0, 2.0, 3.0]})
    result = fill_missing(df, 'a', 'mean')
    assert len(st.session_state['cleaning_log']) == 0


# drop_missing tests
def test_drop_missing_removes_rows():
    df = pd.DataFrame({'a': [1.0, None, 3.0]})
    result = drop_missing(df, 'a')
    assert len(result) == 2


def test_drop_missing_logs():
    df = pd.DataFrame({'a': [1.0, None, 3.0]})
    drop_missing(df, 'a')
    assert len(st.session_state['cleaning_log']) == 1
    assert st.session_state['cleaning_log'][0]['action'] == 'drop_missing'
    assert st.session_state['cleaning_log'][0]['column'] == 'a'
    assert st.session_state['cleaning_log'][0]['rows_dropped'] == 1


def test_drop_missing_no_nulls():
    df = pd.DataFrame({'a': [1.0, 2.0, 3.0]})
    result = drop_missing(df, 'a')
    assert len(result) == 3
    assert len(st.session_state['cleaning_log']) == 0


# clip_outliers tests
def test_clip_outliers():
    df = pd.DataFrame({'val': [1.0, 2.0, 1000.0]})
    result = clip_outliers(df, 'val', 0.0, 10.0)
    assert result['val'].max() <= 10.0


def test_clip_outliers_logs():
    df = pd.DataFrame({'val': [1.0, 2.0, 1000.0]})
    clip_outliers(df, 'val', 0.0, 10.0)
    assert len(st.session_state['cleaning_log']) == 1
    assert st.session_state['cleaning_log'][0]['action'] == 'clip_outliers'
    assert st.session_state['cleaning_log'][0]['clipped_count'] == 1


def test_clip_outliers_no_outliers():
    df = pd.DataFrame({'val': [1.0, 2.0, 5.0]})
    result = clip_outliers(df, 'val', 0.0, 10.0)
    assert len(st.session_state['cleaning_log']) == 0


# cast_column tests
def test_cast_column_to_int():
    df = pd.DataFrame({'a': [1.0, 2.0, 3.0]})
    result = cast_column(df, 'a', 'int')
    assert result['a'].dtype == 'Int64'


def test_cast_column_logs():
    df = pd.DataFrame({'a': [1.0, 2.0, 3.0]})
    cast_column(df, 'a', 'int')
    assert len(st.session_state['cleaning_log']) == 1
    assert st.session_state['cleaning_log'][0]['action'] == 'cast_column'
    assert st.session_state['cleaning_log'][0]['column'] == 'a'


def test_cast_column_to_str():
    df = pd.DataFrame({'a': [1, 2, 3]})
    result = cast_column(df, 'a', 'str')
    assert result['a'].dtype == 'str'


# normalize_text tests
def test_normalize_text_lowercase():
    df = pd.DataFrame({'a': ['HELLO', 'WORLD']})
    result = normalize_text(df, 'a', 'lowercase')
    assert result['a'].iloc[0] == 'hello'


def test_normalize_text_logs():
    df = pd.DataFrame({'a': ['HELLO', 'WORLD']})
    normalize_text(df, 'a', 'lowercase')
    assert len(st.session_state['cleaning_log']) == 1
    assert st.session_state['cleaning_log'][0]['action'] == 'normalize_text'
    assert st.session_state['cleaning_log'][0]['operation'] == 'lowercase'


def test_normalize_text_uppercase():
    df = pd.DataFrame({'a': ['hello', 'world']})
    result = normalize_text(df, 'a', 'uppercase')
    assert result['a'].iloc[0] == 'HELLO'


def test_normalize_text_strip():
    df = pd.DataFrame({'a': ['  hello  ', '  world  ']})
    result = normalize_text(df, 'a', 'strip_whitespace')
    assert result['a'].iloc[0] == 'hello'


# Edge case tests
def test_empty_dataframe():
    df = pd.DataFrame()
    result = drop_duplicates(df)
    assert len(result) == 0


def test_missing_column_raises():
    df = pd.DataFrame({'a': [1, 2, 3]})
    with pytest.raises(KeyError):
        fill_missing(df, 'b', 'mean')
