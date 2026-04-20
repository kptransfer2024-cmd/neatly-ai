"""Tests for app.py helper functions."""
import pandas as pd
import pytest
import streamlit as st

if 'cleaning_log' not in st.session_state:
    st.session_state['cleaning_log'] = []

from app import _actions_for, _humanize


# --- _humanize tests ---

def test_humanize_single_word():
    assert _humanize('duplicates') == 'Duplicates'


def test_humanize_snake_case():
    assert _humanize('missing_value') == 'Missing Value'


def test_humanize_multiple_underscores():
    assert _humanize('inconsistent_format') == 'Inconsistent Format'


def test_humanize_all_caps():
    assert _humanize('TYPE_MISMATCH') == 'Type Mismatch'


def test_humanize_empty_string():
    assert _humanize('') == ''


# --- _actions_for tests ---

def test_actions_for_missing_value_numeric():
    """Missing value on numeric column should offer mean, median, mode, drop."""
    issue = {
        'type': 'missing_value',
        'column': 'age',
        'dtype': 'float64',
        'missing_count': 5,
    }
    actions = _actions_for(issue)
    labels = [label for label, _ in actions]
    assert 'Fill mean' in labels
    assert 'Fill median' in labels
    assert 'Fill mode' in labels
    assert 'Drop rows' in labels


def test_actions_for_missing_value_categorical():
    """Missing value on categorical column should offer mode and drop (no mean/median)."""
    issue = {
        'type': 'missing_value',
        'column': 'color',
        'dtype': 'object',
        'missing_count': 3,
    }
    actions = _actions_for(issue)
    labels = [label for label, _ in actions]
    assert 'Fill mode' in labels
    assert 'Drop rows' in labels
    assert 'Fill mean' not in labels
    assert 'Fill median' not in labels


def test_actions_for_missing_value_str_dtype():
    """Missing value on str dtype should also offer mode and drop (no mean/median)."""
    issue = {
        'type': 'missing_value',
        'column': 'name',
        'dtype': 'str',
        'missing_count': 2,
    }
    actions = _actions_for(issue)
    labels = [label for label, _ in actions]
    assert 'Fill mode' in labels
    assert 'Drop rows' in labels
    assert 'Fill mean' not in labels


def test_actions_for_missing_value_no_column():
    """Missing value issue without column should return no actions."""
    issue = {
        'type': 'missing_value',
        'missing_count': 5,
    }
    actions = _actions_for(issue)
    assert actions == []


def test_actions_for_duplicates():
    """Duplicates issue should offer drop duplicate rows."""
    issue = {
        'type': 'duplicates',
        'duplicate_count': 10,
    }
    actions = _actions_for(issue)
    labels = [label for label, _ in actions]
    assert 'Drop duplicate rows' in labels


def test_actions_for_outliers_with_fences():
    """Outliers with lower/upper fences should offer clip to IQR."""
    issue = {
        'type': 'outliers',
        'column': 'salary',
        'lower_fence': 10000.0,
        'upper_fence': 500000.0,
        'outlier_count': 3,
    }
    actions = _actions_for(issue)
    labels = [label for label, _ in actions]
    assert 'Clip to IQR fence' in labels


def test_actions_for_outliers_missing_fences():
    """Outliers without fences should return no actions."""
    issue = {
        'type': 'outliers',
        'column': 'salary',
        'outlier_count': 3,
    }
    actions = _actions_for(issue)
    assert actions == []


def test_actions_for_outliers_no_column():
    """Outliers issue without column should return no actions."""
    issue = {
        'type': 'outliers',
        'lower_fence': 10000.0,
        'upper_fence': 500000.0,
    }
    actions = _actions_for(issue)
    assert actions == []


def test_actions_for_type_mismatch_with_target():
    """Type mismatch with suggested_dtype should offer cast."""
    issue = {
        'type': 'type_mismatch',
        'column': 'age',
        'current_dtype': 'object',
        'suggested_dtype': 'numeric',
    }
    actions = _actions_for(issue)
    labels = [label for label, _ in actions]
    assert 'Cast to numeric' in labels


def test_actions_for_type_mismatch_no_target():
    """Type mismatch without suggested_dtype should return no actions."""
    issue = {
        'type': 'type_mismatch',
        'column': 'age',
        'current_dtype': 'object',
    }
    actions = _actions_for(issue)
    assert actions == []


def test_actions_for_type_mismatch_no_column():
    """Type mismatch without column should return no actions."""
    issue = {
        'type': 'type_mismatch',
        'suggested_dtype': 'numeric',
    }
    actions = _actions_for(issue)
    assert actions == []


def test_actions_for_inconsistent_format_whitespace():
    """Inconsistent whitespace should offer strip."""
    issue = {
        'type': 'inconsistent_format',
        'column': 'name',
        'sub_type': 'extra_whitespace',
    }
    actions = _actions_for(issue)
    labels = [label for label, _ in actions]
    assert 'Strip whitespace' in labels


def test_actions_for_inconsistent_format_mixed_case():
    """Inconsistent case should offer lowercase and titlecase."""
    issue = {
        'type': 'inconsistent_format',
        'column': 'status',
        'sub_type': 'mixed_case',
    }
    actions = _actions_for(issue)
    labels = [label for label, _ in actions]
    assert 'Lowercase' in labels
    assert 'Titlecase' in labels


def test_actions_for_inconsistent_format_date():
    """Inconsistent date format should offer cast to datetime."""
    issue = {
        'type': 'inconsistent_format',
        'column': 'birthdate',
        'sub_type': 'mixed_date_format',
    }
    actions = _actions_for(issue)
    labels = [label for label, _ in actions]
    assert 'Cast to datetime' in labels


def test_actions_for_inconsistent_format_no_column():
    """Inconsistent format without column should return no actions."""
    issue = {
        'type': 'inconsistent_format',
        'sub_type': 'extra_whitespace',
    }
    actions = _actions_for(issue)
    assert actions == []


def test_actions_for_inconsistent_format_unknown_subtype():
    """Inconsistent format with unknown sub_type should return no actions."""
    issue = {
        'type': 'inconsistent_format',
        'column': 'name',
        'sub_type': 'unknown_subtype',
    }
    actions = _actions_for(issue)
    assert actions == []


def test_actions_for_unknown_issue_type():
    """Unknown issue type should return no actions."""
    issue = {
        'type': 'unknown_issue',
        'column': 'col',
    }
    actions = _actions_for(issue)
    assert actions == []


def test_actions_for_missing_type_key():
    """Issue without 'type' key should return no actions."""
    issue = {
        'column': 'age',
        'value': 123,
    }
    actions = _actions_for(issue)
    assert actions == []


# --- Action handler tests (verify handlers work with real data) ---

def test_action_handler_fill_mean():
    """Verify fill mean action handler works."""
    issue = {
        'type': 'missing_value',
        'column': 'value',
        'dtype': 'float64',
    }
    actions = _actions_for(issue)
    fill_mean_action = next((label, handler) for label, handler in actions if 'mean' in label.lower())
    label, handler = fill_mean_action

    df = pd.DataFrame({'value': [1.0, 2.0, None, 4.0]})
    log = []
    result = handler(df, log)

    assert result['value'].isna().sum() == 0
    assert len(log) == 1
    assert log[0]['action'] == 'fill_missing'


def test_action_handler_drop_duplicates():
    """Verify drop duplicates action handler works."""
    issue = {
        'type': 'duplicates',
        'duplicate_count': 1,
    }
    actions = _actions_for(issue)
    label, handler = actions[0]

    df = pd.DataFrame({'a': [1, 1, 2]})
    log = []
    result = handler(df, log)

    assert len(result) == 2
    assert len(log) == 1


def test_action_handler_clip_outliers():
    """Verify clip outliers action handler works."""
    issue = {
        'type': 'outliers',
        'column': 'salary',
        'lower_fence': 30000.0,
        'upper_fence': 200000.0,
    }
    actions = _actions_for(issue)
    label, handler = actions[0]

    df = pd.DataFrame({'salary': [10000.0, 50000.0, 500000.0]})
    log = []
    result = handler(df, log)

    assert result['salary'].min() >= 30000.0
    assert result['salary'].max() <= 200000.0


def test_action_handler_normalize_whitespace():
    """Verify normalize whitespace action handler works."""
    issue = {
        'type': 'inconsistent_format',
        'column': 'name',
        'sub_type': 'extra_whitespace',
    }
    actions = _actions_for(issue)
    label, handler = actions[0]

    df = pd.DataFrame({'name': ['  Alice  ', 'Bob', '  Charlie  ']})
    log = []
    result = handler(df, log)

    assert result['name'].iloc[0] == 'Alice'
    assert result['name'].iloc[2] == 'Charlie'


def test_action_handler_cast_to_datetime():
    """Verify cast to datetime action handler works."""
    issue = {
        'type': 'inconsistent_format',
        'column': 'date',
        'sub_type': 'mixed_date_format',
    }
    actions = _actions_for(issue)
    label, handler = actions[0]

    df = pd.DataFrame({'date': ['2024-01-15', '2024-02-15']})
    log = []
    result = handler(df, log)

    assert 'datetime' in str(result['date'].dtype)


# --- Integration tests ---

def test_multiple_issues_different_types():
    """Verify actions are available for all issue types simultaneously."""
    issues = [
        {'type': 'missing_value', 'column': 'age', 'dtype': 'float64'},
        {'type': 'duplicates', 'duplicate_count': 5},
        {'type': 'outliers', 'column': 'salary', 'lower_fence': 10000, 'upper_fence': 500000},
    ]
    all_actions = []
    for issue in issues:
        all_actions.extend(_actions_for(issue))

    assert len(all_actions) >= 3  # At least one action per issue type


def test_no_actions_for_malformed_issues():
    """Verify no actions for completely malformed issues."""
    bad_issues = [
        {},
        {'type': None},
        {'column': 'age'},
        {'some_random_key': 'value'},
    ]
    for issue in bad_issues:
        assert _actions_for(issue) == []


# --- New detector action tests ---

def test_actions_for_near_duplicates():
    """Near duplicates should offer merge and flag actions."""
    issue = {
        'type': 'near_duplicates',
        'column': 'name',
        'row_indices': [0, 1, 2],
    }
    actions = _actions_for(issue)
    labels = [label for label, _ in actions]
    assert 'Merge cluster' in labels
    assert 'Flag cluster' in labels


def test_actions_for_near_duplicates_no_indices():
    """Near duplicates without row_indices should return no actions."""
    issue = {
        'type': 'near_duplicates',
        'column': 'name',
    }
    actions = _actions_for(issue)
    assert actions == []


def test_actions_for_pattern_mismatch_email():
    """Pattern mismatch should offer flag and drop actions."""
    issue = {
        'type': 'pattern_mismatch',
        'column': 'email',
        'sample_data': {
            'email': {'pattern': 'email'}
        },
    }
    actions = _actions_for(issue)
    labels = [label for label, _ in actions]
    assert 'Flag invalid' in labels
    assert 'Drop invalid rows' in labels


def test_actions_for_pattern_mismatch_no_pattern():
    """Pattern mismatch without pattern should return no actions."""
    issue = {
        'type': 'pattern_mismatch',
        'column': 'email',
        'sample_data': {},
    }
    actions = _actions_for(issue)
    assert actions == []


def test_actions_for_out_of_range():
    """Out of range should offer clip and drop actions."""
    issue = {
        'type': 'out_of_range',
        'column': 'age',
        'sample_data': {
            'age': {'valid_lo': 0.0, 'valid_hi': 150.0}
        },
    }
    actions = _actions_for(issue)
    labels = [label for label, _ in actions]
    assert 'Clip to range' in labels
    assert 'Drop invalid rows' in labels


def test_actions_for_out_of_range_only_bounds():
    """Out of range with only one bound should still offer actions."""
    issue = {
        'type': 'out_of_range',
        'column': 'price',
        'sample_data': {
            'price': {'valid_lo': 0.0, 'valid_hi': None}
        },
    }
    actions = _actions_for(issue)
    assert len(actions) == 2


def test_handler_merge_near_duplicates():
    """Verify merge near duplicates handler works."""
    from transformation_executor import merge_near_duplicates
    issue = {
        'type': 'near_duplicates',
        'column': 'name',
        'row_indices': [0, 1],
    }
    actions = _actions_for(issue)
    merge_action = next(a for a in actions if 'Merge' in a[0])
    label, handler = merge_action

    df = pd.DataFrame({'name': ['Alice', 'Alice2', 'Bob']})
    log = []
    result = handler(df, log)

    assert len(result) == 2


def test_handler_flag_invalid_patterns():
    """Verify flag invalid patterns handler works."""
    issue = {
        'type': 'pattern_mismatch',
        'column': 'email',
        'sample_data': {
            'email': {'pattern': 'email'}
        },
    }
    actions = _actions_for(issue)
    flag_action = next(a for a in actions if 'Flag' in a[0])
    label, handler = flag_action

    df = pd.DataFrame({'email': ['test@example.com', 'invalid', 'user@domain.org']})
    log = []
    result = handler(df, log)

    assert pd.isna(result['email'].iloc[1])


def test_handler_drop_out_of_range():
    """Verify drop out of range handler works."""
    issue = {
        'type': 'out_of_range',
        'column': 'age',
        'sample_data': {
            'age': {'valid_lo': 0.0, 'valid_hi': 150.0}
        },
    }
    actions = _actions_for(issue)
    drop_action = next(a for a in actions if 'Drop' in a[0])
    label, handler = drop_action

    df = pd.DataFrame({'age': [25, 200, 35]})
    log = []
    result = handler(df, log)

    assert len(result) == 2
