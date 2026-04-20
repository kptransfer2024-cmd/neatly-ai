from datetime import date, timedelta
import pandas as pd

from detectors.date_range_validator import detect


# --- empty / no-match ---

def test_empty_dataframe_returns_empty():
    assert detect(pd.DataFrame()) == []


def test_no_datetime_columns_returns_empty():
    df = pd.DataFrame({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})
    assert detect(df) == []


def test_in_range_dates_not_flagged():
    df = pd.DataFrame({
        'created_at': pd.to_datetime(['2020-01-15', '2023-06-30', '2024-11-04']),
    })
    assert detect(df) == []


def test_small_column_skipped():
    df = pd.DataFrame({'d': pd.to_datetime(['2024-01-01'])})
    assert detect(df) == []


def test_non_datetime_dtype_ignored():
    # String dates are schema_analyzer's job — this detector only scans true datetime columns.
    df = pd.DataFrame({'d': ['2024-01-01', '2500-01-01']})
    assert detect(df) == []


# --- detection ---

def test_year_before_1900_flagged():
    df = pd.DataFrame({
        'event_date': pd.to_datetime(['1899-12-30', '2023-01-01', '2024-06-15']),
    })
    issues = detect(df)
    assert len(issues) == 1
    issue = issues[0]
    assert issue['type'] == 'date_out_of_range'
    assert issue['columns'] == ['event_date']
    assert issue['sample_data']['event_date']['before_lower_count'] == 1


def test_year_after_2100_flagged():
    df = pd.DataFrame({
        'scheduled_for': pd.to_datetime(['2024-01-01', '2200-05-10', '2023-07-07']),
    })
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['sample_data']['scheduled_for']['after_upper_count'] == 1


def test_birth_date_future_flagged():
    tomorrow = (date.today() + timedelta(days=1)).isoformat()
    df = pd.DataFrame({
        'birth_date': pd.to_datetime(['1990-05-15', tomorrow, '2001-03-22']),
    })
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['sub_type'] == 'birth_date_out_of_range'
    assert issues[0]['sample_data']['birth_date']['after_upper_count'] == 1


def test_dob_hint_recognized():
    tomorrow = (date.today() + timedelta(days=1)).isoformat()
    df = pd.DataFrame({
        'patient_dob': pd.to_datetime(['1985-01-01', tomorrow, '1970-06-15']),
    })
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['sub_type'] == 'birth_date_out_of_range'


def test_non_birth_future_within_2100_not_flagged():
    # A non-birth column with a date in the future but still before year 2100 is allowed.
    future = '2099-01-01'
    df = pd.DataFrame({
        'scheduled_for': pd.to_datetime(['2024-01-01', future, '2025-06-15']),
    })
    assert detect(df) == []


def test_both_tails_counted():
    df = pd.DataFrame({
        'd': pd.to_datetime(['1800-01-01', '2024-01-01', '2500-01-01']),
    })
    issues = detect(df)
    data = issues[0]['sample_data']['d']
    assert data['before_lower_count'] == 1
    assert data['after_upper_count'] == 1


def test_nat_ignored():
    df = pd.DataFrame({
        'd': pd.to_datetime(['1800-01-01', None, '2024-01-01']),
    })
    issues = detect(df)
    # NaT shouldn't count as out-of-range; only the pre-1900 value does.
    assert issues[0]['sample_data']['d']['before_lower_count'] == 1


# --- severity ---

def test_severity_low_for_one_in_many():
    # 1 bad out of 101 = < 2% → low
    bad = ['1800-01-01']
    good = [f'2020-01-{(i % 28) + 1:02d}' for i in range(100)]
    df = pd.DataFrame({'d': pd.to_datetime(bad + good)})
    issues = detect(df)
    assert issues[0]['severity'] == 'low'


def test_severity_high_above_10pct():
    # 3 bad out of 10 = 30% → high
    bad = ['1800-01-01', '1800-01-02', '1800-01-03']
    good = [f'2020-01-0{i}' for i in range(1, 8)]
    df = pd.DataFrame({'d': pd.to_datetime(bad + good)})
    issues = detect(df)
    assert issues[0]['severity'] == 'high'


# --- issue shape ---

def test_issue_has_canonical_schema():
    df = pd.DataFrame({'d': pd.to_datetime(['1800-01-01', '2024-01-01'])})
    issue = detect(df)[0]
    required = {'detector', 'type', 'columns', 'severity', 'row_indices',
                'summary', 'sample_data', 'actions'}
    assert required.issubset(issue.keys())
    assert issue['detector'] == 'date_range_validator'


def test_action_has_drop_out_of_range_dates():
    df = pd.DataFrame({'d': pd.to_datetime(['1800-01-01', '2024-01-01'])})
    issue = detect(df)[0]
    action = issue['actions'][0]
    assert action['id'] == 'drop_out_of_range_dates'
    assert action['params']['column'] == 'd'
    assert 'lower' in action['params'] and 'upper' in action['params']


def test_row_indices_capped_at_five():
    bad = [f'1800-01-{i:02d}' for i in range(1, 11)]  # 10 bad dates
    good = ['2020-01-01'] * 5
    df = pd.DataFrame({'d': pd.to_datetime(bad + good)})
    issue = detect(df)[0]
    assert len(issue['row_indices']) == 5
