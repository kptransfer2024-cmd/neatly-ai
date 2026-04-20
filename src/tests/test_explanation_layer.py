from explanation_layer import _explain_one, _fallback_explanation, explain_issues


# --- explain_issues ---

def test_empty_issues_returns_empty():
    assert explain_issues([], {}) == []


def test_attaches_explanation_key():
    issues = [{'type': 'missing_value', 'columns': ['age'], 'missing_count': 10, 'missing_pct': 5.0}]
    result = explain_issues(issues, {})
    assert 'explanation' in result[0]
    assert result[0]['explanation'] != ''


def test_mutates_and_returns_same_list():
    issues = [{'type': 'missing_value', 'columns': ['x'], 'missing_count': 1, 'missing_pct': 1.0}]
    result = explain_issues(issues, {})
    assert result is issues
    assert 'explanation' in issues[0]


def test_df_stats_ignored_no_error():
    issues = [{'type': 'missing_value', 'columns': ['x'], 'missing_count': 3, 'missing_pct': 3.0}]
    explain_issues(issues, {'rows': 100, 'cols': 5})  # df_stats ignored, must not raise


def test_multiple_issues_all_get_explanation():
    issues = [
        {'type': 'missing_value', 'columns': ['a'], 'missing_count': 5, 'missing_pct': 5.0},
        {'type': 'outliers', 'columns': ['b'], 'outlier_count': 2, 'outlier_pct': 1.0,
         'lower_fence': -10.0, 'upper_fence': 100.0},
    ]
    result = explain_issues(issues, {})
    assert all('explanation' in i for i in result)


# --- template interpolation ---

def test_missing_value_template_interpolates_stats():
    issue = {'type': 'missing_value', 'columns': ['age'], 'missing_count': 42, 'missing_pct': 8.4}
    text = _explain_one(issue)
    assert 'age' in text
    assert '42' in text
    assert '8.4' in text


def test_outlier_template_interpolates_fence():
    issue = {
        'type': 'outliers', 'columns': ['price'],
        'outlier_count': 3, 'outlier_pct': 1.5,
        'lower_fence': -5.0, 'upper_fence': 200.0,
    }
    text = _explain_one(issue)
    assert 'price' in text
    assert '3' in text
    assert '-5.0' in text
    assert '200.0' in text


def test_duplicate_template_uses_duplicate_count():
    issue = {'type': 'duplicates', 'columns': [], 'duplicate_count': 7}
    text = _explain_one(issue)
    assert '7' in text


def test_duplicate_column_reads_duplicate_of_from_sample_data():
    issue = {
        'type': 'duplicate_column',
        'columns': ['col_b'],
        'sample_data': {'col_b': {'duplicate_of': 'col_a'}},
    }
    text = _explain_one(issue)
    assert 'col_b' in text
    assert 'col_a' in text


def test_whitespace_template_interpolates():
    issue = {
        'type': 'whitespace_values', 'columns': ['notes'],
        'whitespace_count': 12, 'whitespace_pct': 6.0,
    }
    text = _explain_one(issue)
    assert 'notes' in text
    assert '12' in text


def test_mixed_type_template_interpolates():
    issue = {
        'type': 'mixed_type', 'columns': ['revenue'],
        'dirty_count': 5, 'dirty_pct': 10.0,
    }
    text = _explain_one(issue)
    assert 'revenue' in text
    assert '5' in text


def test_constant_column_template_uses_col_name():
    issue = {'type': 'constant_column', 'columns': ['status']}
    text = _explain_one(issue)
    assert 'status' in text


def test_unknown_type_falls_back():
    issue = {'type': 'some_new_unknown_type', 'columns': ['x']}
    text = _explain_one(issue)
    assert 'x' in text
    assert 'some_new_unknown_type' in text


def test_missing_template_key_falls_back_gracefully():
    # Template requires missing_count but it's absent — must not crash
    issue = {'type': 'missing_value', 'columns': ['y']}
    text = _explain_one(issue)
    assert isinstance(text, str)
    assert len(text) > 0


def test_columns_list_used_for_col():
    issue = {'type': 'constant_column', 'columns': ['my_col']}
    text = _explain_one(issue)
    assert 'my_col' in text


def test_column_singular_fallback():
    # Some detectors emit 'column' (str) instead of 'columns' (list)
    issue = {'type': 'constant_column', 'column': 'old_col', 'columns': []}
    text = _explain_one(issue)
    assert 'old_col' in text


# --- _fallback_explanation ---

def test_fallback_with_column():
    assert _fallback_explanation({'type': 'missing', 'column': 'age'}) == (
        "Detected missing in column 'age'."
    )


def test_fallback_with_columns_list():
    assert _fallback_explanation({'type': 'missing', 'columns': ['age']}) == (
        "Detected missing in column 'age'."
    )


def test_fallback_without_column():
    assert _fallback_explanation({'type': 'duplicates'}) == 'Detected duplicates.'
