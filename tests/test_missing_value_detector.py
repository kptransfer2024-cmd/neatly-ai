import pandas as pd
import pytest
from detectors.missing_value_detector import detect_missing, suggest_strategy


def test_no_missing_returns_empty():
    pass


def test_numeric_30pct_suggests_fill_median():
    pass


def test_60pct_missing_suggests_drop_column():
    pass


def test_object_30pct_suggests_fill_mode():
    pass


def test_all_null_column():
    pass
