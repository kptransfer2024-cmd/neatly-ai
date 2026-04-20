import pandas as pd
import pytest
from detectors.duplicate_detector import detect


def test_no_duplicates():
    df = pd.DataFrame({'a': [1, 2, 3]})
    assert detect(df) == []


def test_detects_duplicates():
    df = pd.DataFrame({'a': [1, 1, 2]})
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['type'] == 'duplicates'
    assert issues[0]['duplicate_count'] == 1
    assert issues[0]['total_rows'] == 3
