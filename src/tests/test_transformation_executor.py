import pandas as pd
import pytest

from transformation_executor import (
    drop_duplicates,
    fill_missing,
    drop_missing,
    clip_outliers,
    cast_column,
    normalize_text,
)


# drop_duplicates tests
def test_drop_duplicates_removes_rows():
    df = pd.DataFrame({'a': [1, 1, 2]})
    log = []
    result = drop_duplicates(df, log)
    assert len(result) == 2


def test_drop_duplicates_logs():
    df = pd.DataFrame({'a': [1, 1, 2]})
    log = []
    drop_duplicates(df, log)
    assert len(log) == 1
    assert log[0]['action'] == 'drop_duplicates'
    assert log[0]['duplicates_removed'] == 1


def test_drop_duplicates_no_dupes():
    df = pd.DataFrame({'a': [1, 2, 3]})
    log = []
    result = drop_duplicates(df, log)
    assert len(result) == 3
    assert len(log) == 0


# fill_missing tests
def test_fill_missing_mean():
    df = pd.DataFrame({'a': [1.0, None, 3.0]})
    log = []
    result = fill_missing(df, 'a', 'mean', log)
    assert result['a'].isna().sum() == 0


def test_fill_missing_logs():
    df = pd.DataFrame({'a': [1.0, None, 3.0]})
    log = []
    fill_missing(df, 'a', 'mean', log)
    assert len(log) == 1
    assert log[0]['action'] == 'fill_missing'
    assert log[0]['column'] == 'a'
    assert log[0]['filled_count'] == 1


def test_fill_missing_median():
    df = pd.DataFrame({'a': [1.0, None, 100.0]})
    log = []
    result = fill_missing(df, 'a', 'median', log)
    assert result['a'].isna().sum() == 0


def test_fill_missing_mode():
    df = pd.DataFrame({'a': ['x', 'x', None, 'y']})
    log = []
    result = fill_missing(df, 'a', 'mode', log)
    assert result['a'].isna().sum() == 0


def test_fill_missing_constant():
    df = pd.DataFrame({'a': [1.0, None, 3.0]})
    log = []
    result = fill_missing(df, 'a', 'constant', log, fill_value=999)
    assert result['a'].isna().sum() == 0
    assert result['a'].iloc[1] == 999


def test_fill_missing_invalid_strategy():
    df = pd.DataFrame({'a': [1.0, None, 3.0]})
    log = []
    with pytest.raises(ValueError):
        fill_missing(df, 'a', 'invalid', log)


def test_fill_missing_no_nulls():
    df = pd.DataFrame({'a': [1.0, 2.0, 3.0]})
    log = []
    result = fill_missing(df, 'a', 'mean', log)
    assert len(log) == 0


# drop_missing tests
def test_drop_missing_removes_rows():
    df = pd.DataFrame({'a': [1.0, None, 3.0]})
    log = []
    result = drop_missing(df, 'a', log)
    assert len(result) == 2


def test_drop_missing_logs():
    df = pd.DataFrame({'a': [1.0, None, 3.0]})
    log = []
    drop_missing(df, 'a', log)
    assert len(log) == 1
    assert log[0]['action'] == 'drop_missing'
    assert log[0]['column'] == 'a'
    assert log[0]['rows_dropped'] == 1


def test_drop_missing_no_nulls():
    df = pd.DataFrame({'a': [1.0, 2.0, 3.0]})
    log = []
    result = drop_missing(df, 'a', log)
    assert len(result) == 3
    assert len(log) == 0


# clip_outliers tests
def test_clip_outliers():
    df = pd.DataFrame({'val': [1.0, 2.0, 1000.0]})
    log = []
    result = clip_outliers(df, 'val', 0.0, 10.0, log)
    assert result['val'].max() <= 10.0


def test_clip_outliers_logs():
    df = pd.DataFrame({'val': [1.0, 2.0, 1000.0]})
    log = []
    clip_outliers(df, 'val', 0.0, 10.0, log)
    assert len(log) == 1
    assert log[0]['action'] == 'clip_outliers'
    assert log[0]['clipped_count'] == 1


def test_clip_outliers_no_outliers():
    df = pd.DataFrame({'val': [1.0, 2.0, 5.0]})
    log = []
    result = clip_outliers(df, 'val', 0.0, 10.0, log)
    assert len(log) == 0


# cast_column tests
def test_cast_column_to_int():
    df = pd.DataFrame({'a': [1.0, 2.0, 3.0]})
    log = []
    result = cast_column(df, 'a', 'int', log)
    assert result['a'].dtype == 'Int64'


def test_cast_column_logs():
    df = pd.DataFrame({'a': [1.0, 2.0, 3.0]})
    log = []
    cast_column(df, 'a', 'int', log)
    assert len(log) == 1
    assert log[0]['action'] == 'cast_column'
    assert log[0]['column'] == 'a'


def test_cast_column_to_str():
    df = pd.DataFrame({'a': [1, 2, 3]})
    log = []
    result = cast_column(df, 'a', 'str', log)
    assert result['a'].dtype == 'str'


# normalize_text tests
def test_normalize_text_lowercase():
    df = pd.DataFrame({'a': ['HELLO', 'WORLD']})
    log = []
    result = normalize_text(df, 'a', 'lowercase', log)
    assert result['a'].iloc[0] == 'hello'


def test_normalize_text_logs():
    df = pd.DataFrame({'a': ['HELLO', 'WORLD']})
    log = []
    normalize_text(df, 'a', 'lowercase', log)
    assert len(log) == 1
    assert log[0]['action'] == 'normalize_text'
    assert log[0]['operation'] == 'lowercase'


def test_normalize_text_uppercase():
    df = pd.DataFrame({'a': ['hello', 'world']})
    log = []
    result = normalize_text(df, 'a', 'uppercase', log)
    assert result['a'].iloc[0] == 'HELLO'


def test_normalize_text_strip():
    df = pd.DataFrame({'a': ['  hello  ', '  world  ']})
    log = []
    result = normalize_text(df, 'a', 'strip_whitespace', log)
    assert result['a'].iloc[0] == 'hello'


# Edge case tests
def test_empty_dataframe():
    df = pd.DataFrame()
    log = []
    result = drop_duplicates(df, log)
    assert len(result) == 0


def test_missing_column_raises():
    df = pd.DataFrame({'a': [1, 2, 3]})
    log = []
    with pytest.raises(KeyError):
        fill_missing(df, 'b', 'mean', log)


def test_cast_to_datetime():
    df = pd.DataFrame({'d': ['2024-01-15', '2024-02-15']})
    log = []
    result = cast_column(df, 'd', 'datetime', log)
    assert 'datetime' in str(result['d'].dtype)


def test_cast_noop_does_not_log():
    df = pd.DataFrame({'a': [1, 2, 3]})  # already int64
    log = []
    cast_column(df, 'a', 'int64', log)
    assert log == []


def test_normalize_titlecase():
    df = pd.DataFrame({'a': ['hello world', 'foo bar']})
    log = []
    result = normalize_text(df, 'a', 'titlecase', log)
    assert result['a'].iloc[0] == 'Hello World'


def test_clip_outliers_caps_low():
    df = pd.DataFrame({'v': [-1000.0, 5.0, 8.0]})
    log = []
    result = clip_outliers(df, 'v', 0.0, 10.0, log)
    assert result['v'].min() == 0.0


def test_multiple_actions_accumulate_in_log():
    df = pd.DataFrame({'a': [1.0, 1.0, None, 1000.0]})
    log = []
    df = drop_duplicates(df, log)
    df = fill_missing(df, 'a', 'mean', log)
    df = clip_outliers(df, 'a', 0.0, 10.0, log)
    actions = [entry['action'] for entry in log]
    assert actions == ['drop_duplicates', 'fill_missing', 'clip_outliers']


# --- merge_near_duplicates tests ---

def test_merge_near_duplicates_removes_extra_rows():
    from transformation_executor import merge_near_duplicates
    df = pd.DataFrame({'name': ['Alice', 'Bob', 'Charlie']})
    log = []
    result = merge_near_duplicates(df, log, 'name', [0, 1])
    assert len(result) == 2


def test_merge_near_duplicates_logs():
    from transformation_executor import merge_near_duplicates
    df = pd.DataFrame({'name': ['Alice', 'Bob', 'Charlie']})
    log = []
    merge_near_duplicates(df, log, 'name', [0, 1, 2])
    assert len(log) == 1
    assert log[0]['action'] == 'merge_near_duplicates'
    assert log[0]['rows_merged'] == 2


def test_merge_near_duplicates_keeps_first():
    from transformation_executor import merge_near_duplicates
    df = pd.DataFrame({'name': ['Alice', 'Alice2', 'Bob']})
    log = []
    result = merge_near_duplicates(df, log, 'name', [0, 1])
    assert result['name'].iloc[0] == 'Alice'


# --- flag_near_duplicates tests ---

def test_flag_near_duplicates_adds_column():
    from transformation_executor import flag_near_duplicates
    df = pd.DataFrame({'name': ['Alice', 'Bob', 'Charlie']})
    log = []
    result = flag_near_duplicates(df, log, 'name', [0, 2])
    assert 'name_near_duplicate_flag' in result.columns


def test_flag_near_duplicates_marks_rows():
    from transformation_executor import flag_near_duplicates
    df = pd.DataFrame({'name': ['Alice', 'Bob', 'Charlie']})
    log = []
    result = flag_near_duplicates(df, log, 'name', [0, 2])
    assert result['name_near_duplicate_flag'].iloc[0] == True
    assert result['name_near_duplicate_flag'].iloc[1] == False
    assert result['name_near_duplicate_flag'].iloc[2] == True


# --- merge_all_near_duplicates (bulk) tests ---

def test_merge_all_near_duplicates_drops_extras_from_every_cluster():
    from transformation_executor import merge_all_near_duplicates
    df = pd.DataFrame({'name': ['Alice', 'Alice2', 'Bob', 'Bob2', 'Carol', 'Dana']})
    log = []
    clusters = [
        {'column': 'name', 'row_indices': [0, 1]},
        {'column': 'name', 'row_indices': [2, 3]},
    ]
    result = merge_all_near_duplicates(df, log, clusters)
    assert len(result) == 4  # 6 - 2 dropped
    assert list(result['name']) == ['Alice', 'Bob', 'Carol', 'Dana']


def test_merge_all_near_duplicates_logs_once():
    from transformation_executor import merge_all_near_duplicates
    df = pd.DataFrame({'name': ['A', 'A2', 'B', 'B2']})
    log = []
    merge_all_near_duplicates(df, log, [
        {'column': 'name', 'row_indices': [0, 1]},
        {'column': 'name', 'row_indices': [2, 3]},
    ])
    assert len(log) == 1
    entry = log[0]
    assert entry['action'] == 'merge_all_near_duplicates'
    assert entry['clusters_merged'] == 2
    assert entry['rows_merged'] == 2


def test_merge_all_near_duplicates_empty_clusters_noop():
    from transformation_executor import merge_all_near_duplicates
    df = pd.DataFrame({'a': [1, 2, 3]})
    log = []
    result = merge_all_near_duplicates(df, log, [])
    assert len(result) == 3
    assert log == []


def test_merge_all_near_duplicates_singleton_cluster_ignored():
    # A "cluster" of 1 row isn't a cluster — skip it
    from transformation_executor import merge_all_near_duplicates
    df = pd.DataFrame({'a': [1, 2, 3]})
    log = []
    result = merge_all_near_duplicates(df, log, [{'column': 'a', 'row_indices': [0]}])
    assert len(result) == 3
    assert log == []


# --- flag_all_near_duplicates (bulk) tests ---

def test_flag_all_near_duplicates_adds_single_flag_column():
    from transformation_executor import flag_all_near_duplicates
    df = pd.DataFrame({'name': ['A', 'B', 'C', 'D']})
    log = []
    result = flag_all_near_duplicates(df, log, [
        {'column': 'name', 'row_indices': [0, 1]},
        {'column': 'name', 'row_indices': [2, 3]},
    ])
    assert 'near_duplicate_flag' in result.columns
    assert result['near_duplicate_flag'].sum() == 4  # all 4 flagged


def test_flag_all_near_duplicates_only_marks_listed_rows():
    from transformation_executor import flag_all_near_duplicates
    df = pd.DataFrame({'name': ['A', 'B', 'C', 'D']})
    log = []
    result = flag_all_near_duplicates(df, log, [
        {'column': 'name', 'row_indices': [1, 3]},
    ])
    assert list(result['near_duplicate_flag']) == [False, True, False, True]


def test_flag_all_near_duplicates_logs():
    from transformation_executor import flag_all_near_duplicates
    df = pd.DataFrame({'name': ['A', 'B']})
    log = []
    flag_all_near_duplicates(df, log, [{'column': 'name', 'row_indices': [0, 1]}])
    assert log[0]['action'] == 'flag_all_near_duplicates'
    assert log[0]['flagged_count'] == 2


# --- flag_invalid_patterns tests ---

def test_flag_invalid_patterns_email():
    from transformation_executor import flag_invalid_patterns
    df = pd.DataFrame({'email': ['test@example.com', 'invalid', 'user@domain.org']})
    log = []
    result = flag_invalid_patterns(df, log, 'email', 'email')
    assert pd.isna(result['email'].iloc[1])
    assert result['email'].iloc[0] == 'test@example.com'


def test_flag_invalid_patterns_logs():
    from transformation_executor import flag_invalid_patterns
    df = pd.DataFrame({'email': ['test@example.com', 'invalid', 'user@domain.org']})
    log = []
    flag_invalid_patterns(df, log, 'email', 'email')
    assert len(log) == 1
    assert log[0]['action'] == 'flag_invalid_patterns'
    assert log[0]['flagged_as_null_count'] == 1


def test_flag_invalid_patterns_zip():
    from transformation_executor import flag_invalid_patterns
    df = pd.DataFrame({'zip': ['12345', 'invalid', '98765-4321']})
    log = []
    result = flag_invalid_patterns(df, log, 'zip', 'us_zip')
    assert pd.isna(result['zip'].iloc[1])


# --- drop_invalid_rows tests ---

def test_drop_invalid_rows_email():
    from transformation_executor import drop_invalid_rows
    df = pd.DataFrame({'email': ['test@example.com', 'invalid', 'user@domain.org']})
    log = []
    result = drop_invalid_rows(df, log, 'email', 'email')
    assert len(result) == 2


def test_drop_invalid_rows_logs():
    from transformation_executor import drop_invalid_rows
    df = pd.DataFrame({'email': ['test@example.com', 'invalid', 'user@domain.org']})
    log = []
    drop_invalid_rows(df, log, 'email', 'email')
    assert len(log) == 1
    assert log[0]['action'] == 'drop_invalid_rows'
    assert log[0]['rows_dropped'] == 1


# --- drop_out_of_range_rows tests ---

def test_drop_out_of_range_rows_age():
    from transformation_executor import drop_out_of_range_rows
    df = pd.DataFrame({'age': [25, 200, 35]})
    log = []
    result = drop_out_of_range_rows(df, log, 'age', 0, 150)
    assert len(result) == 2


def test_drop_out_of_range_rows_logs():
    from transformation_executor import drop_out_of_range_rows
    df = pd.DataFrame({'age': [25, 200, 35]})
    log = []
    drop_out_of_range_rows(df, log, 'age', 0, 150)
    assert len(log) == 1
    assert log[0]['action'] == 'drop_out_of_range_rows'
    assert log[0]['rows_dropped'] == 1


def test_drop_out_of_range_rows_only_lower_bound():
    from transformation_executor import drop_out_of_range_rows
    df = pd.DataFrame({'price': [-10, 25, 100]})
    log = []
    result = drop_out_of_range_rows(df, log, 'price', 0, None)
    assert len(result) == 2
    assert result['price'].min() >= 0


def test_drop_out_of_range_rows_only_upper_bound():
    from transformation_executor import drop_out_of_range_rows
    df = pd.DataFrame({'score': [50, 150, 80]})
    log = []
    result = drop_out_of_range_rows(df, log, 'score', None, 100)
    assert len(result) == 2
    assert result['score'].max() <= 100


# --- drop_column tests ---

def test_drop_column_removes_column():
    from transformation_executor import drop_column
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    log = []
    result = drop_column(df, 'a', log)
    assert list(result.columns) == ['b']
    assert len(result) == 3


def test_drop_column_logs():
    from transformation_executor import drop_column
    df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    log = []
    drop_column(df, 'a', log)
    assert log[0]['action'] == 'drop_column'
    assert log[0]['column'] == 'a'
    assert log[0]['columns_before'] == 2
    assert log[0]['columns_after'] == 1


def test_drop_column_missing_column_raises():
    from transformation_executor import drop_column
    df = pd.DataFrame({'a': [1]})
    log = []
    with pytest.raises(KeyError):
        drop_column(df, 'missing', log)


def test_drop_column_preserves_input():
    from transformation_executor import drop_column
    df = pd.DataFrame({'a': [1], 'b': [2]})
    log = []
    drop_column(df, 'a', log)
    # Original untouched
    assert list(df.columns) == ['a', 'b']


# --- clip_to_range tests ---

def test_clip_to_range_clamps_high():
    from transformation_executor import clip_to_range
    df = pd.DataFrame({'score': [50, 150, 80]})
    log = []
    result = clip_to_range(df, log, 'score', 0, 100)
    assert result['score'].max() == 100
    assert result.loc[1, 'score'] == 100  # 150 clamped to 100


def test_clip_to_range_clamps_low():
    from transformation_executor import clip_to_range
    df = pd.DataFrame({'score': [50, -10, 80]})
    log = []
    result = clip_to_range(df, log, 'score', 0, 100)
    assert result['score'].min() == 0
    assert result.loc[1, 'score'] == 0  # -10 clamped to 0


def test_clip_to_range_in_bounds_unchanged():
    from transformation_executor import clip_to_range
    df = pd.DataFrame({'score': [50, 75, 80]})
    log = []
    result = clip_to_range(df, log, 'score', 0, 100)
    assert (result['score'] == df['score']).all()


def test_clip_to_range_nan_unchanged():
    from transformation_executor import clip_to_range
    df = pd.DataFrame({'score': [50.0, float('nan'), 80.0]})
    log = []
    result = clip_to_range(df, log, 'score', 0, 100)
    assert pd.isna(result.loc[1, 'score'])
    assert result.loc[0, 'score'] == 50.0
    assert result.loc[2, 'score'] == 80.0


def test_clip_to_range_one_sided_hi_none():
    from transformation_executor import clip_to_range
    df = pd.DataFrame({'score': [-5, 50, 150]})
    log = []
    result = clip_to_range(df, log, 'score', 0, None)
    assert result.loc[0, 'score'] == 0  # -5 clamped to 0
    assert result.loc[1, 'score'] == 50  # unchanged
    assert result.loc[2, 'score'] == 150  # no upper bound


def test_clip_to_range_one_sided_lo_none():
    from transformation_executor import clip_to_range
    df = pd.DataFrame({'score': [-5, 50, 150]})
    log = []
    result = clip_to_range(df, log, 'score', None, 100)
    assert result.loc[0, 'score'] == -5  # no lower bound
    assert result.loc[1, 'score'] == 50  # unchanged
    assert result.loc[2, 'score'] == 100  # 150 clamped to 100


def test_clip_to_range_logs_correctly():
    from transformation_executor import clip_to_range
    df = pd.DataFrame({'score': [50, 150, -5]})
    log = []
    clip_to_range(df, log, 'score', 0, 100)
    assert log[0]['action'] == 'clip_to_range'
    assert log[0]['column'] == 'score'
    assert log[0]['valid_range'] == [0, 100]
    assert log[0]['values_clipped'] == 2  # 150 and -5


# --- null_out_whitespace tests ---

def test_null_out_whitespace_basic():
    from transformation_executor import null_out_whitespace
    df = pd.DataFrame({'a': ['  ', 'hello', '   ']})
    log = []
    result = null_out_whitespace(df, log, 'a')
    assert pd.isna(result['a'].iloc[0])
    assert result['a'].iloc[1] == 'hello'
    assert pd.isna(result['a'].iloc[2])
    assert log[0]['nulled_count'] == 2


def test_null_out_whitespace_noop():
    from transformation_executor import null_out_whitespace
    df = pd.DataFrame({'a': ['x', 'y']})
    log = []
    result = null_out_whitespace(df, log, 'a')
    assert result['a'].iloc[0] == 'x'
    assert log == []


def test_null_out_whitespace_tabs_newlines():
    from transformation_executor import null_out_whitespace
    df = pd.DataFrame({'a': ['\t', '\n', 'real']})
    log = []
    result = null_out_whitespace(df, log, 'a')
    assert pd.isna(result['a'].iloc[0])
    assert pd.isna(result['a'].iloc[1])
    assert result['a'].iloc[2] == 'real'
    assert log[0]['nulled_count'] == 2


def test_null_out_whitespace_missing_column():
    from transformation_executor import null_out_whitespace
    df = pd.DataFrame({'a': ['x']})
    log = []
    with pytest.raises(KeyError):
        null_out_whitespace(df, log, 'missing')


# --- drop_whitespace_rows tests ---

def test_drop_whitespace_rows_basic():
    from transformation_executor import drop_whitespace_rows
    df = pd.DataFrame({'a': ['  ', 'hello', ' ']})
    log = []
    result = drop_whitespace_rows(df, log, 'a')
    assert len(result) == 1
    assert result['a'].iloc[0] == 'hello'
    assert log[0]['rows_dropped'] == 2


def test_drop_whitespace_rows_noop():
    from transformation_executor import drop_whitespace_rows
    df = pd.DataFrame({'a': ['x', 'y']})
    log = []
    result = drop_whitespace_rows(df, log, 'a')
    assert len(result) == 2
    assert log == []


def test_drop_whitespace_rows_preserves_nan():
    from transformation_executor import drop_whitespace_rows
    df = pd.DataFrame({'a': [None, '  ', 'ok']})
    log = []
    result = drop_whitespace_rows(df, log, 'a')
    assert len(result) == 2  # NaN row stays, whitespace row dropped
    assert pd.isna(result['a'].iloc[0])
    assert result['a'].iloc[1] == 'ok'


def test_drop_whitespace_rows_missing_column():
    from transformation_executor import drop_whitespace_rows
    df = pd.DataFrame({'a': ['x']})
    log = []
    with pytest.raises(KeyError):
        drop_whitespace_rows(df, log, 'missing')


# --- coerce_to_numeric tests ---

def test_coerce_to_numeric_dirty_strings():
    from transformation_executor import coerce_to_numeric
    df = pd.DataFrame({'rev': ['100', 'N/A', '200', 'bad']})
    log = []
    result = coerce_to_numeric(df, log, 'rev')
    assert result['rev'].iloc[0] == 100.0
    assert pd.isna(result['rev'].iloc[1])
    assert result['rev'].iloc[2] == 200.0
    assert pd.isna(result['rev'].iloc[3])
    assert log[0]['values_coerced_to_null'] == 2


def test_coerce_to_numeric_already_numeric():
    from transformation_executor import coerce_to_numeric
    df = pd.DataFrame({'n': [1.0, 2.0, 3.0]})
    log = []
    result = coerce_to_numeric(df, log, 'n')
    assert log == []


def test_coerce_to_numeric_missing_column():
    from transformation_executor import coerce_to_numeric
    df = pd.DataFrame({'n': [1]})
    log = []
    with pytest.raises(KeyError):
        coerce_to_numeric(df, log, 'missing')


# --- drop_non_numeric_rows tests ---

def test_drop_non_numeric_rows_basic():
    from transformation_executor import drop_non_numeric_rows
    df = pd.DataFrame({'rev': ['100', 'N/A', '200']})
    log = []
    result = drop_non_numeric_rows(df, log, 'rev')
    assert len(result) == 2
    assert result['rev'].iloc[0] == '100'
    assert result['rev'].iloc[1] == '200'
    assert log[0]['rows_dropped'] == 1


def test_drop_non_numeric_rows_preserves_nan():
    from transformation_executor import drop_non_numeric_rows
    df = pd.DataFrame({'rev': [None, '100', 'bad']})
    log = []
    result = drop_non_numeric_rows(df, log, 'rev')
    assert len(result) == 2  # None row stays, 'bad' dropped
    assert pd.isna(result['rev'].iloc[0])
    assert result['rev'].iloc[1] == '100'


def test_drop_non_numeric_rows_noop():
    from transformation_executor import drop_non_numeric_rows
    df = pd.DataFrame({'n': ['1', '2', '3']})
    log = []
    result = drop_non_numeric_rows(df, log, 'n')
    assert len(result) == 3
    assert log == []


def test_drop_non_numeric_rows_missing_column():
    from transformation_executor import drop_non_numeric_rows
    df = pd.DataFrame({'n': [1]})
    log = []
    with pytest.raises(KeyError):
        drop_non_numeric_rows(df, log, 'missing')
