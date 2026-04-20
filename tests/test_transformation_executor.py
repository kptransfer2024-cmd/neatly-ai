import pandas as pd
import pytest
import streamlit as st

# Bootstrap minimal session_state before importing the module
if 'cleaning_log' not in st.session_state:
    st.session_state['cleaning_log'] = []

from transformation_executor import drop_duplicates, fill_missing, drop_missing, clip_outliers


@pytest.fixture(autouse=True)
def clear_log():
    st.session_state['cleaning_log'] = []
    yield


def test_drop_duplicates_removes_rows():
    df = pd.DataFrame({'a': [1, 1, 2]})
    result = drop_duplicates(df)
    assert len(result) == 2


def test_drop_duplicates_logs():
    df = pd.DataFrame({'a': [1, 1, 2]})
    drop_duplicates(df)
    assert len(st.session_state['cleaning_log']) == 1


def test_fill_missing_mean():
    df = pd.DataFrame({'a': [1.0, None, 3.0]})
    result = fill_missing(df, 'a', 'mean')
    assert result['a'].isna().sum() == 0


def test_clip_outliers():
    df = pd.DataFrame({'val': [1.0, 2.0, 1000.0]})
    result = clip_outliers(df, 'val', 0.0, 10.0)
    assert result['val'].max() <= 10.0
