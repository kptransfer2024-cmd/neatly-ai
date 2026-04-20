import pandas as pd

from detectors.id_column_detector import detect


# --- empty / no-match ---

def test_empty_dataframe_returns_empty():
    assert detect(pd.DataFrame()) == []


def test_no_columns_returns_empty():
    assert detect(pd.DataFrame(index=[0, 1])) == []


def test_regular_data_not_flagged():
    df = pd.DataFrame({'age': [25, 30, 35, 40], 'name': ['a', 'b', 'c', 'd']})
    assert detect(df) == []


def test_small_column_skipped():
    # fewer than 4 rows → insufficient signal
    df = pd.DataFrame({'id': [1, 2, 3]})
    assert detect(df) == []


def test_numeric_with_gaps_not_flagged():
    # not consecutive → not sequential_integer
    df = pd.DataFrame({'id': [1, 2, 5, 10]})
    assert detect(df) == []


def test_non_unique_column_not_flagged():
    df = pd.DataFrame({'id': [1, 2, 2, 3]})
    assert detect(df) == []


# --- sequential integer ---

def test_sequential_integer_detected():
    df = pd.DataFrame({'row_id': [1, 2, 3, 4, 5]})
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['sub_type'] == 'sequential_integer'
    assert issues[0]['columns'] == ['row_id']


def test_sequential_integer_not_starting_at_one():
    df = pd.DataFrame({'order_id': [1001, 1002, 1003, 1004, 1005]})
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['sub_type'] == 'sequential_integer'


def test_sequential_integer_out_of_order_detected():
    # insertion order doesn't matter — sort and check consecutive
    df = pd.DataFrame({'id': [4, 1, 3, 2, 5]})
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['sub_type'] == 'sequential_integer'


def test_float_whole_numbers_count_as_sequential():
    # pd.to_numeric gives float64 for some inputs; whole-number floats still qualify
    df = pd.DataFrame({'id': [1.0, 2.0, 3.0, 4.0]})
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['sub_type'] == 'sequential_integer'


def test_real_floats_not_sequential():
    df = pd.DataFrame({'price': [1.5, 2.7, 3.14, 4.1]})
    assert detect(df) == []


# --- UUID ---

def test_uuid_column_detected():
    df = pd.DataFrame({'session_id': [
        '550e8400-e29b-41d4-a716-446655440000',
        'f47ac10b-58cc-4372-a567-0e02b2c3d479',
        '6ba7b810-9dad-11d1-80b4-00c04fd430c8',
        '6ba7b811-9dad-11d1-80b4-00c04fd430c8',
    ]})
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['sub_type'] == 'uuid'


def test_uuid_case_insensitive():
    df = pd.DataFrame({'id': [
        '550E8400-E29B-41D4-A716-446655440000',
        'F47AC10B-58CC-4372-A567-0E02B2C3D479',
        '6BA7B810-9DAD-11D1-80B4-00C04FD430C8',
        '6BA7B811-9DAD-11D1-80B4-00C04FD430C8',
    ]})
    issues = detect(df)
    assert issues[0]['sub_type'] == 'uuid'


def test_non_uuid_strings_not_flagged():
    df = pd.DataFrame({'name': ['alice-bob', 'foo-bar', 'baz-qux', 'hello']})
    assert detect(df) == []


# --- hex ID ---

def test_mongo_objectid_detected():
    df = pd.DataFrame({'oid': [
        '507f1f77bcf86cd799439011',
        '507f191e810c19729de860ea',
        '5f50c31e8a7c8e0001a5a7b1',
        '60d5ec49f6e43f001fabc123',
    ]})
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['sub_type'] == 'hex_id'


def test_sha_hash_detected():
    df = pd.DataFrame({'commit': [
        'a' * 40, 'b' * 40, 'c' * 40, 'd' * 40,  # 40-char sha1-like
    ]})
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['sub_type'] == 'hex_id'


def test_short_hex_not_flagged():
    # < 24 chars → not ID-like
    df = pd.DataFrame({'x': ['abcd1234', 'ef567890', '12345678', '87654321']})
    assert detect(df) == []


# --- issue shape ---

def test_issue_has_canonical_schema():
    df = pd.DataFrame({'id': [1, 2, 3, 4]})
    issue = detect(df)[0]
    required = {'detector', 'type', 'columns', 'severity', 'row_indices',
                'summary', 'sample_data', 'actions'}
    assert required.issubset(issue.keys())
    assert issue['detector'] == 'id_column_detector'
    assert issue['severity'] == 'medium'


def test_action_is_drop_column():
    df = pd.DataFrame({'id': [1, 2, 3, 4]})
    issue = detect(df)[0]
    assert issue['actions'][0]['id'] == 'drop_column'
    assert issue['actions'][0]['params'] == {'column': 'id'}


def test_multiple_id_columns_all_flagged():
    df = pd.DataFrame({
        'row_id': [1, 2, 3, 4],
        'uuid': [
            '550e8400-e29b-41d4-a716-446655440000',
            'f47ac10b-58cc-4372-a567-0e02b2c3d479',
            '6ba7b810-9dad-11d1-80b4-00c04fd430c8',
            '6ba7b811-9dad-11d1-80b4-00c04fd430c8',
        ],
        'age': [25, 30, 35, 40],
    })
    issues = detect(df)
    flagged = {i['columns'][0] for i in issues}
    assert flagged == {'row_id', 'uuid'}


def test_nan_rows_ignored_in_cardinality_check():
    # 4 unique + 1 NaN → still 100% unique among non-nulls → flagged if sequential
    df = pd.DataFrame({'id': [1, 2, 3, 4, None]})
    issues = detect(df)
    assert len(issues) == 1
    assert issues[0]['sub_type'] == 'sequential_integer'
