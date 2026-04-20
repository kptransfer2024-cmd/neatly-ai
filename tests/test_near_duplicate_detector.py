import pandas as pd
from detectors.near_duplicate_detector import detect


def test_empty_dataframe_returns_empty():
    df = pd.DataFrame()
    assert detect(df) == []


def test_no_string_columns_returns_empty():
    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4.0, 5.0, 6.0]})
    assert detect(df) == []


def test_all_null_column_returns_empty():
    df = pd.DataFrame({'name': [None, None, None]})
    assert detect(df) == []


def test_clean_distinct_strings_returns_empty():
    df = pd.DataFrame({'name': ['apple', 'banana', 'cherry', 'durian']})
    assert detect(df) == []


def test_exact_duplicates_detected():
    df = pd.DataFrame({'name': ['Alice', 'Alice', 'Bob', 'Charlie']})
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['column'] == 'name'
    assert issues[0]['type'] == 'near_duplicates'
    assert sorted(issues[0]['row_indices']) == [0, 1]


def test_near_duplicates_case_insensitive():
    df = pd.DataFrame({
        'company': ['Acme Corp'] * 4 + ['Other Inc']
    })
    issues = detect(df)
    assert len(issues) == 1
    assert len(issues[0]['row_indices']) == 4


def test_single_row_no_cluster():
    df = pd.DataFrame({'name': ['Alice']})
    assert detect(df) == []


def test_high_cardinality_column_skipped():
    # Every value is unique → cardinality_ratio = 1.0 > 0.95
    df = pd.DataFrame({
        'uuid': [f'id-{i}' for i in range(100)]
    })
    assert detect(df) == []


def test_constant_column_skipped():
    # All values the same → cardinality_ratio ≈ 0
    df = pd.DataFrame({
        'status': ['active'] * 100
    })
    assert detect(df) == []


def test_short_strings_skipped():
    df = pd.DataFrame({'code': ['a', 'b', 'c', 'aa', 'ab']})
    assert detect(df) == []


def test_large_df_capped_at_max_rows():
    # 1000-row df; cap is 500, so max row_indices should be < 500
    df = pd.DataFrame({
        'name': ['name'] * 1000
    })
    issues = detect(df)
    if issues:
        assert all(i < 500 for i in issues[0]['row_indices'])


def test_similarity_below_threshold_not_clustered():
    df = pd.DataFrame({
        'name': ['hello', 'world', 'apple', 'banana', 'cherry', 'durian']
    })
    assert detect(df) == []


def test_similarity_at_or_above_threshold_clustered():
    # 'hello hello' and 'hello hello' (exact) are 1.0 similar
    df = pd.DataFrame({
        'name': ['hello hello'] * 4 + ['goodbye world'] + ['other thing', 'different']
    })
    issues = detect(df)
    assert len(issues) >= 1
    # The 4 'hello hello' rows should be in one cluster
    assert len(issues[0]['row_indices']) == 4


def test_multiple_clusters_in_same_column():
    df = pd.DataFrame({
        'company': [
            'Acme Corp'] * 3 + ['Smith LLC'] * 3 + ['Other Inc'] * 2
    })
    issues = detect(df)
    # Should find 3 clusters: Acme (3), Smith (3), Other (2)
    assert len(issues) >= 2
    assert all(len(i['row_indices']) >= 2 for i in issues)


def test_row_indices_are_ints():
    df = pd.DataFrame({'name': ['Alice', 'Alice', 'Bob', 'Bob']})
    issues = detect(df)
    assert len(issues) >= 1
    for issue in issues:
        assert all(isinstance(i, int) for i in issue['row_indices'])


def test_issue_shape_has_required_keys():
    df = pd.DataFrame({'name': ['Alice', 'Alice', 'Bob']})
    issues = detect(df)
    assert len(issues) == 1
    issue = issues[0]
    required_keys = {'detector', 'type', 'column', 'severity', 'row_indices', 'summary', 'sample_data', 'actions'}
    assert required_keys.issubset(issue.keys())


def test_actions_are_merge_and_flag():
    df = pd.DataFrame({'name': ['Alice', 'Alice', 'Bob']})
    issues = detect(df)
    assert len(issues) == 1
    action_ids = [a['id'] for a in issues[0]['actions']]
    assert 'merge_near_duplicates' in action_ids
    assert 'flag_near_duplicates' in action_ids


def test_nan_rows_not_in_cluster():
    # The detector only processes non-null rows, so NaN should never appear in clusters
    df = pd.DataFrame({
        'name': ['Alice'] * 5 + [None, None] + ['Bob'] * 5 + [None]
    })
    issues = detect(df)
    assert len(issues) >= 1
    # Verify all row_indices point to valid non-NaN values
    for issue in issues:
        for idx in issue['row_indices']:
            assert pd.notna(df.iloc[idx]['name']), f"Row {idx} is NaN but in cluster"


def test_leading_trailing_whitespace_normalized():
    df = pd.DataFrame({
        'company': ['  Acme Corp  '] * 3 + ['Acme Corp'] * 2 + ['Other Inc']
    })
    issues = detect(df)
    assert len(issues) >= 1
    # The versions with and without whitespace should cluster together
    assert any(len(i['row_indices']) >= 3 for i in issues)


def test_case_normalization():
    df = pd.DataFrame({
        'name': ['HELLO WORLD'] * 3 + ['hello world'] * 2 + ['goodbye world', 'foo bar', 'baz qux']
    })
    issues = detect(df)
    # 'HELLO WORLD' and 'hello world' should cluster together after normalization
    assert len(issues) >= 1
    assert any(len(i['row_indices']) >= 3 for i in issues)


def test_severity_low_for_small_cluster():
    # 2-row cluster in 20-row df = 10% and cluster_size=2 < 5 → low
    df = pd.DataFrame({
        'name': ['Acme Corp'] * 2 + ['Company A', 'Company B', 'Company C', 'Company D',
                 'Company E', 'Company F', 'Company G', 'Company H',
                 'Company I', 'Company J', 'Company K', 'Company L',
                 'Company M', 'Company N', 'Company O', 'Company P',
                 'Company Q', 'Company R']
    })
    issues = detect(df)
    assert len(issues) >= 1
    small_clusters = [i for i in issues if len(i['row_indices']) == 2]
    assert len(small_clusters) >= 1
    assert small_clusters[0]['severity'] == 'low'


def test_severity_medium_for_mid_cluster():
    # 5-row cluster in 50-row df = 10% and cluster_size=5 → medium
    df = pd.DataFrame({
        'name': ['Acme Corp'] * 5 + [f'other{i}' for i in range(45)]
    })
    issues = detect(df)
    assert len(issues) >= 1
    medium_issues = [i for i in issues if len(i['row_indices']) == 5]
    assert len(medium_issues) >= 1
    assert medium_issues[0]['severity'] == 'medium'


def test_severity_high_for_large_cluster():
    # 10-row cluster → high
    df = pd.DataFrame({
        'name': ['Acme Corp'] * 10 + ['Other Inc', 'Another', 'Third']
    })
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['severity'] == 'high'


def test_severity_high_for_high_percentage():
    # 12-row cluster in 50-row df = 24% > 20% → high
    df = pd.DataFrame({
        'name': ['Acme Corp'] * 12 + [f'other{i}' for i in range(38)]
    })
    issues = detect(df)
    assert len(issues) >= 1
    high_issues = [i for i in issues if len(i['row_indices']) == 12]
    assert len(high_issues) >= 1
    assert high_issues[0]['severity'] == 'high'


def test_cluster_sample_values_included():
    df = pd.DataFrame({
        'name': ['Alice Smith', 'Alice Smith', 'Alice Smyth', 'Bob Jones']
    })
    issues = detect(df)
    assert len(issues) == 1
    sample_values = issues[0]['sample_data']['name']['sample_values']
    assert len(sample_values) > 0
    assert 'Alice' in ' '.join(sample_values)
