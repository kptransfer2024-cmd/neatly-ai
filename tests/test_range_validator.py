import pandas as pd
from detectors.range_validator import detect


def test_empty_dataframe_returns_empty():
    df = pd.DataFrame()
    assert detect(df) == []


def test_no_numeric_columns_returns_empty():
    df = pd.DataFrame({'x': ['a', 'b', 'c'], 'y': ['d', 'e', 'f']})
    assert detect(df) == []


def test_no_domain_columns_returns_empty():
    df = pd.DataFrame({'x': [1, 2, 3], 'value': [4.0, 5.0, 6.0]})
    assert detect(df) == []


def test_clean_age_column_returns_empty():
    df = pd.DataFrame({'age': [25, 30, 45, 60]})
    assert detect(df) == []


def test_detects_negative_age():
    df = pd.DataFrame({'age': [-1, 25, 30, 45]})
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['column'] == 'age'
    assert issues[0]['type'] == 'out_of_range'
    assert 0 in issues[0]['row_indices']


def test_detects_age_above_150():
    df = pd.DataFrame({'age': [999, 25, 30, 45]})
    issues = detect(df)
    assert len(issues) == 1
    assert 0 in issues[0]['row_indices']
    assert issues[0]['sample_data']['age']['min_violation'] == 999.0


def test_detects_percentage_above_100():
    df = pd.DataFrame({'success_rate': [0.5, 0.7, 150, 0.9]})
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['column'] == 'success_rate'


def test_detects_negative_price():
    df = pd.DataFrame({'unit_price': [10.0, -5.0, 20.0, 15.0]})
    issues = detect(df)
    assert len(issues) == 1
    assert 1 in issues[0]['row_indices']


def test_price_no_upper_bound():
    df = pd.DataFrame({'price': [10.0, 50.0, 999999.0]})
    assert detect(df) == []


def test_year_below_1900():
    df = pd.DataFrame({'birth_year': [1800, 1950, 2000, 2010]})
    issues = detect(df)
    assert len(issues) == 1
    assert 0 in issues[0]['row_indices']


def test_year_above_2100():
    df = pd.DataFrame({'year': [2000, 2010, 2020, 2200]})
    issues = detect(df)
    assert len(issues) == 1
    assert 3 in issues[0]['row_indices']


def test_rating_above_5():
    df = pd.DataFrame({'rating': [1.0, 2.5, 4.5, 6.0]})
    issues = detect(df)
    assert len(issues) == 1
    assert 3 in issues[0]['row_indices']


def test_rating_below_0():
    df = pd.DataFrame({'rating': [-1.0, 2.5, 4.5, 5.0]})
    issues = detect(df)
    assert len(issues) == 1
    assert 0 in issues[0]['row_indices']


def test_score_at_boundary_not_flagged():
    df = pd.DataFrame({'score': [0, 50, 100]})
    assert detect(df) == []


def test_all_null_column_skipped():
    df = pd.DataFrame({'age': [None, None, None]})
    assert detect(df) == []


def test_severity_high_above_20pct():
    # 5 total, 2 violations = 40%
    df = pd.DataFrame({'age': [-1, 25, -2, 45, 60]})
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['severity'] == 'high'


def test_severity_medium_above_5pct():
    # 20 total, 2 violations = 10%
    df = pd.DataFrame({
        'age': [25, 30, 45, 60, 70, 80, 90, 100, 110, 120, 25, 30, 45, 60, 70, 80, 90, -1, -2, 55]
    })
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['severity'] == 'medium'


def test_severity_low_below_5pct():
    # 100 total, 2 violations = 2%
    df = pd.DataFrame({
        'age': list(range(25, 125)) + [-1, -2]
    })
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['severity'] == 'low'


def test_fuzzy_name_match_product_price_usd():
    df = pd.DataFrame({'product_price_usd': [10.0, -5.0, 20.0]})
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['sample_data']['product_price_usd']['domain_keyword'] == 'price'


def test_fuzzy_name_match_total_count():
    df = pd.DataFrame({'item_count': [1, -1, 5]})
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['sample_data']['item_count']['domain_keyword'] == 'count'


def test_issue_shape_has_required_keys():
    df = pd.DataFrame({'age': [25, -1, 45]})
    issues = detect(df)
    assert len(issues) == 1
    issue = issues[0]
    required_keys = {'detector', 'type', 'column', 'severity', 'row_indices', 'summary', 'sample_data', 'actions'}
    assert required_keys.issubset(issue.keys())


def test_actions_contain_clip_and_drop():
    df = pd.DataFrame({'age': [25, -1, 45]})
    issues = detect(df)
    assert len(issues) == 1
    action_ids = [a['id'] for a in issues[0]['actions']]
    assert 'clip_to_range' in action_ids
    assert 'drop_out_of_range_rows' in action_ids


def test_row_indices_are_ints():
    df = pd.DataFrame({'age': [25, -1, 45, 200]})
    issues = detect(df)
    assert len(issues) == 1
    assert all(isinstance(i, int) for i in issues[0]['row_indices'])


def test_multiple_domain_columns_each_flagged():
    df = pd.DataFrame({
        'age': [25, -1, 45],
        'rating': [1.0, 2.5, 6.0],
    })
    issues = detect(df)
    assert len(issues) == 2
    age_issue = [i for i in issues if i['column'] == 'age'][0]
    rating_issue = [i for i in issues if i['column'] == 'rating'][0]
    assert age_issue['sample_data']['age']['domain_keyword'] == 'age'
    assert rating_issue['sample_data']['rating']['domain_keyword'] == 'rating'


def test_column_named_score_integer_dtype():
    df = pd.DataFrame({'score': [10, -5, 100, 50]}, dtype='int64')
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['column'] == 'score'


def test_qty_alias_for_quantity():
    df = pd.DataFrame({'qty': [-1, 5, 10]})
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['sample_data']['qty']['domain_keyword'] == 'qty'


def test_pct_alias_for_percent():
    df = pd.DataFrame({'discount_pct': [50, 101, 25]})
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['sample_data']['discount_pct']['domain_keyword'] == 'pct'
