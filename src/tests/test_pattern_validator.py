import pandas as pd
from detectors.pattern_validator import detect


def test_empty_dataframe_returns_empty():
    df = pd.DataFrame()
    assert detect(df) == []


def test_no_string_columns_returns_empty():
    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4.0, 5.0, 6.0]})
    assert detect(df) == []


def test_all_null_column_skipped():
    df = pd.DataFrame({'email': [None, None, None]})
    assert detect(df) == []


def test_clean_email_column_returns_empty():
    df = pd.DataFrame({'email': ['alice@example.com', 'bob@test.org', 'charlie@site.net']})
    assert detect(df) == []


def test_low_match_rate_column_not_typed():
    # Only 30% match email regex (below _MIN_COLUMN_MATCH_RATE = 0.6)
    df = pd.DataFrame({'email': ['valid@email.com', 'random text', 'more random', 'even more']})
    assert detect(df) == []


def test_single_invalid_below_min_count():
    # 9 valid emails + 1 bad = invalid_count=1 < _MIN_INVALID_COUNT=2
    df = pd.DataFrame({
        'email': ['a@b.com', 'c@d.com', 'e@f.com', 'g@h.com', 'i@j.com',
                  'k@l.com', 'm@n.com', 'o@p.com', 'q@r.com', 'invalid']
    })
    assert detect(df) == []


def test_detects_invalid_emails():
    df = pd.DataFrame({
        'email': ['valid1@test.com', 'valid2@test.com', 'notanemail', 'also@bad', 'valid3@test.com']
    })
    issues = detect(df)
    assert len(issues) == 1
    issue = issues[0]
    assert issue['columns'] == ['email']
    assert issue['type'] == 'pattern_mismatch'
    assert issue['sample_data']['email']['pattern'] == 'email'
    assert issue['sample_data']['email']['invalid_count'] == 2


def test_detects_invalid_us_phone():
    df = pd.DataFrame({
        'phone': ['(555) 123-4567', '555-123-4567', '555.123.4567', 'notaphone', '+(1) 555-123-4567']
    })
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['sample_data']['phone']['pattern'] == 'us_phone'


def test_detects_invalid_url():
    df = pd.DataFrame({
        'website': ['https://example.com', 'http://test.org', 'ftp://invalid.com', 'not a url', 'https://valid.net']
    })
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['sample_data']['website']['pattern'] == 'url'


def test_detects_invalid_zip():
    df = pd.DataFrame({
        'zip': ['12345', '12345-6789', 'ABCDE', '1234', '54321']
    })
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['sample_data']['zip']['pattern'] == 'us_zip'
    assert issues[0]['sample_data']['zip']['invalid_count'] == 2  # 'ABCDE' and '1234'


def test_severity_high_above_20pct():
    # 5 total, 2 invalid = 40%
    df = pd.DataFrame({'email': ['valid@test.com', 'valid2@test.com', 'valid3@test.com', 'bad1', 'bad2']})
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['severity'] == 'high'


def test_severity_medium_above_5pct():
    # 30 valid + 2 invalid = 32 total (93.75% valid, 6.25% invalid) → medium
    df = pd.DataFrame({
        'email': [f'user{i}@test.com' for i in range(30)] + ['bad1', 'bad2']
    })
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['severity'] == 'medium'
    assert 5 < issues[0]['sample_data']['email']['invalid_pct'] < 20


def test_severity_low_below_5pct():
    # 100 valid + 2 invalid = 1.96% < 5%
    df = pd.DataFrame({
        'email': [f'user{i}@test.com' for i in range(100)] + ['invalid1', 'invalid2']
    })
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['severity'] == 'low'
    assert issues[0]['sample_data']['email']['invalid_pct'] < 5


def test_row_indices_are_integers():
    df = pd.DataFrame({
        'email': ['valid@test.com'] * 6 + ['bad', 'notanemail']
    })
    issues = detect(df)
    assert len(issues) == 1
    assert all(isinstance(i, int) for i in issues[0]['row_indices'])


def test_issue_shape_has_required_keys():
    df = pd.DataFrame({
        'email': ['valid@test.com'] * 7 + ['bad1', 'bad2']
    })
    issues = detect(df)
    assert len(issues) == 1
    issue = issues[0]
    required_keys = {'detector', 'type', 'columns', 'severity', 'row_indices', 'summary', 'sample_data', 'actions'}
    assert required_keys.issubset(issue.keys())


def test_actions_have_required_fields():
    df = pd.DataFrame({
        'email': ['valid@test.com'] * 7 + ['bad1', 'bad2']
    })
    issues = detect(df)
    assert len(issues) == 1
    for action in issues[0]['actions']:
        assert 'id' in action
        assert 'label' in action
        assert 'description' in action
        assert 'params' in action


def test_nan_values_not_flagged():
    df = pd.DataFrame({
        'email': ['valid@test.com'] * 7 + [None, 'bad1', 'bad2', None]
    })
    issues = detect(df)
    assert len(issues) == 1
    # NaN rows should not be in row_indices, only 'bad1' and 'bad2'
    assert len(issues[0]['row_indices']) == 2


def test_multiple_columns_each_independently_detected():
    df = pd.DataFrame({
        'email': ['a@b.com'] * 6 + ['baademail', 'bad'],
        'zip': ['12345'] * 6 + ['ABCDE', 'BAD'],
    })
    issues = detect(df)
    assert len(issues) == 2
    email_issue = [i for i in issues if i['columns'] == ['email']][0]
    zip_issue = [i for i in issues if i['columns'] == ['zip']][0]
    assert email_issue['sample_data']['email']['pattern'] == 'email'
    assert zip_issue['sample_data']['zip']['pattern'] == 'us_zip'


def test_column_with_no_dominant_pattern_skipped():
    # Highly mixed random strings, no pattern ≥60%
    df = pd.DataFrame({
        'mixed': ['hello', 'world', '12345', 'abcde', 'xyz@abc.com', 'test']
    })
    assert detect(df) == []


def test_zip_plus_four_accepted_as_valid():
    df = pd.DataFrame({
        'zip': ['12345'] * 6 + ['12345-6789', '54321-0987', 'BAD1', 'BAD2']
    })
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['sample_data']['zip']['invalid_count'] == 2  # 'BAD1' and 'BAD2'


def test_email_match_rate_in_sample_data():
    df = pd.DataFrame({
        'email': ['valid@test.com'] * 7 + ['bad1', 'bad2']
    })
    issues = detect(df)
    assert len(issues) == 1
    match_rate = issues[0]['sample_data']['email']['match_rate']
    # 7 valid out of 9 = 77.78%
    assert match_rate == 77.77 or abs(match_rate - 77.78) < 0.1
