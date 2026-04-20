"""Tests for data_standardizer detector."""
import pandas as pd
import pytest
from detectors.data_standardizer import detect


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_df(**kwargs) -> pd.DataFrame:
    return pd.DataFrame(kwargs)


def _issue_for(issues: list[dict], col: str) -> dict | None:
    for issue in issues:
        if col in issue.get('columns', []):
            return issue
    return None


# ---------------------------------------------------------------------------
# Edge cases: empty / no-match
# ---------------------------------------------------------------------------

def test_empty_dataframe_returns_no_issues():
    assert detect(pd.DataFrame()) == []


def test_no_string_columns_returns_no_issues():
    df = pd.DataFrame({'nums': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]})
    assert detect(df) == []


def test_column_with_fewer_than_min_samples_skipped():
    df = pd.DataFrame({'phone': ['555-1234'] * 5})  # < 10 samples
    assert detect(df) == []


def test_non_matching_column_name_returns_no_issues():
    df = pd.DataFrame({'description': ['hello world'] * 15})
    assert detect(df) == []


# ---------------------------------------------------------------------------
# Phone detection
# ---------------------------------------------------------------------------

def test_phone_column_detected():
    phones = ['555-123-4567'] * 12
    df = _make_df(phone=phones)
    issues = detect(df)
    issue = _issue_for(issues, 'phone')
    assert issue is not None
    assert issue['sample_data']['phone']['standardization_type'] == 'phone'


def test_phone_column_with_mixed_formats_detected():
    phones = ['(555) 123-4567', '555.123.4567', '5551234567', '555-123-4567'] * 3
    df = _make_df(mobile=phones)
    issues = detect(df)
    assert _issue_for(issues, 'mobile') is not None


def test_tel_keyword_triggers_phone_detection():
    df = _make_df(tel=['555-123-4567'] * 11)
    issues = detect(df)
    assert _issue_for(issues, 'tel') is not None


def test_non_phone_column_name_not_detected():
    df = _make_df(notes=['555-123-4567'] * 12)
    assert _issue_for(detect(df), 'notes') is None


# ---------------------------------------------------------------------------
# Zip code detection
# ---------------------------------------------------------------------------

def test_zip_column_detected():
    zips = ['12345'] * 12
    df = _make_df(zip=zips)
    issues = detect(df)
    issue = _issue_for(issues, 'zip')
    assert issue is not None
    assert issue['sample_data']['zip']['standardization_type'] == 'zip_code'


def test_zip_plus4_detected():
    zips = ['12345-6789'] * 12
    df = _make_df(postal=zips)
    issues = detect(df)
    assert _issue_for(issues, 'postal') is not None


def test_postal_keyword_triggers_zip_detection():
    df = _make_df(postal_code=['90210'] * 11)
    issues = detect(df)
    assert _issue_for(issues, 'postal_code') is not None


# ---------------------------------------------------------------------------
# Date detection
# ---------------------------------------------------------------------------

def test_date_column_iso_detected():
    dates = ['2024-01-15'] * 12
    df = _make_df(date=dates)
    issues = detect(df)
    issue = _issue_for(issues, 'date')
    assert issue is not None
    assert issue['sample_data']['date']['standardization_type'] == 'date'


def test_created_at_column_detected():
    df = _make_df(created_at=['2024-01-15 10:30:00'] * 12)
    issues = detect(df)
    assert _issue_for(issues, 'created_at') is not None


def test_timestamp_column_detected():
    df = _make_df(timestamp=['2024/03/20'] * 12)
    issues = detect(df)
    assert _issue_for(issues, 'timestamp') is not None


def test_non_date_column_not_detected_as_date():
    df = _make_df(name=['Alice'] * 12)
    assert _issue_for(detect(df), 'name') is None


# ---------------------------------------------------------------------------
# Currency detection
# ---------------------------------------------------------------------------

def test_price_column_with_dollar_sign_detected():
    prices = ['$9.99'] * 12
    df = _make_df(price=prices)
    issues = detect(df)
    issue = _issue_for(issues, 'price')
    assert issue is not None
    assert issue['sample_data']['price']['standardization_type'] == 'currency'


def test_cost_column_with_euro_detected():
    df = _make_df(cost=['€12.50'] * 12)
    issues = detect(df)
    assert _issue_for(issues, 'cost') is not None


def test_amount_column_with_plain_numbers_detected():
    df = _make_df(amount=['1234.56'] * 12)
    issues = detect(df)
    assert _issue_for(issues, 'amount') is not None


def test_salary_column_detected():
    df = _make_df(salary=['£50000'] * 12)
    issues = detect(df)
    assert _issue_for(issues, 'salary') is not None


# ---------------------------------------------------------------------------
# Issue schema validation
# ---------------------------------------------------------------------------

def test_issue_schema_has_required_fields():
    df = _make_df(phone=['555-123-4567'] * 12)
    issues = detect(df)
    assert len(issues) == 1
    issue = issues[0]
    assert issue['detector'] == 'data_standardizer'
    assert issue['type'] == 'standardization_suggested'
    assert isinstance(issue['columns'], list)
    assert issue['severity'] in ('low', 'medium', 'high')
    assert isinstance(issue['row_indices'], list)
    assert isinstance(issue['summary'], str)
    assert isinstance(issue['sample_data'], dict)
    assert isinstance(issue['actions'], list)


def test_actions_list_is_non_empty():
    df = _make_df(zip=['12345'] * 12)
    issues = detect(df)
    assert len(issues[0]['actions']) >= 1


def test_sample_data_contains_sample_values():
    df = _make_df(price=['$9.99', '$14.99'] * 6)
    issues = detect(df)
    issue = _issue_for(issues, 'price')
    assert 'sample_values' in issue['sample_data']['price']
    assert len(issue['sample_data']['price']['sample_values']) <= 3


def test_sample_data_contains_suggested_format():
    df = _make_df(phone=['555-123-4567'] * 12)
    issues = detect(df)
    assert 'suggested_format' in issues[0]['sample_data']['phone']


def test_action_has_id_label_description():
    df = _make_df(date=['2024-01-01'] * 12)
    issues = detect(df)
    action = issues[0]['actions'][0]
    assert 'id' in action
    assert 'label' in action
    assert 'description' in action
