import pandas as pd
import pytest
from detectors.outlier_detector import detect


def test_no_outliers():
    df = pd.DataFrame({'val': [10, 11, 12, 10, 11]})
    assert detect(df) == []


def test_detects_outlier():
    df = pd.DataFrame({'val': [10, 11, 12, 10, 1000]})
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['type'] == 'outliers'
    assert issues[0]['column'] == 'val'
    assert issues[0]['outlier_count'] >= 1
