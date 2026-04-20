"""Tests for the codespace sandbox: run_code, validate_package_name,
get_package_version, get_sidebar_package_status, and find_replace."""
import sys
import time

import pandas as pd
import numpy as np
import pytest

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.sandbox import (
    run_code,
    validate_package_name,
    get_package_version,
    get_sidebar_package_status,
    install_package,
)
from transformation_executor import find_replace


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def simple_df():
    return pd.DataFrame({
        'name': ['Alice', 'Bob', 'Charlie', 'alice'],
        'score': [90, 85, 70, 60],
        'city': ['NYC', 'LA', 'NYC', None],
    })


# ---------------------------------------------------------------------------
# validate_package_name
# ---------------------------------------------------------------------------

class TestValidatePackageName:
    def test_valid_simple(self):
        ok, msg = validate_package_name('numpy')
        assert ok
        assert msg == ''

    def test_valid_with_hyphen(self):
        ok, msg = validate_package_name('scikit-learn')
        assert ok

    def test_valid_with_underscore(self):
        ok, msg = validate_package_name('python_dateutil')
        assert ok

    def test_valid_with_dot(self):
        ok, msg = validate_package_name('zope.interface')
        assert ok

    def test_empty_name(self):
        ok, msg = validate_package_name('')
        assert not ok
        assert 'empty' in msg.lower()

    def test_whitespace_only(self):
        ok, msg = validate_package_name('   ')
        assert not ok

    def test_blocked_os(self):
        ok, msg = validate_package_name('os')
        assert not ok
        assert 'not allowed' in msg.lower()

    def test_blocked_subprocess(self):
        ok, msg = validate_package_name('subprocess')
        assert not ok

    def test_blocked_sys(self):
        ok, msg = validate_package_name('sys')
        assert not ok

    def test_blocked_case_insensitive(self):
        ok, msg = validate_package_name('OS')
        assert not ok

    def test_invalid_special_chars(self):
        ok, msg = validate_package_name('my package!')
        assert not ok

    def test_invalid_starts_with_hyphen(self):
        ok, msg = validate_package_name('-bad')
        assert not ok

    def test_strips_whitespace_before_check(self):
        ok, msg = validate_package_name('  numpy  ')
        assert ok


# ---------------------------------------------------------------------------
# run_code — success paths
# ---------------------------------------------------------------------------

class TestRunCodeSuccess:
    def test_returns_result_dict_keys(self, simple_df):
        res = run_code('df = df.copy()', simple_df)
        assert set(res.keys()) == {'ok', 'df', 'stdout', 'error', 'lineno', 'variables', 'elapsed'}

    def test_simple_mutation_ok(self, simple_df):
        res = run_code("df['new_col'] = 1", simple_df)
        assert res['ok'] is True
        assert 'new_col' in res['df'].columns

    def test_filter_rows(self, simple_df):
        res = run_code("df = df[df['score'] > 80]", simple_df)
        assert res['ok'] is True
        assert len(res['df']) == 2

    def test_stdout_captured(self, simple_df):
        res = run_code("print('hello codespace')", simple_df)
        assert res['ok'] is True
        assert 'hello codespace' in res['stdout']

    def test_original_df_not_mutated(self, simple_df):
        original_len = len(simple_df)
        run_code("df = df.head(1)", simple_df)
        assert len(simple_df) == original_len

    def test_elapsed_is_float(self, simple_df):
        res = run_code('pass', simple_df)
        assert isinstance(res['elapsed'], float)
        assert res['elapsed'] >= 0

    def test_variables_extracted(self, simple_df):
        res = run_code("x = 42\ny = 'hello'", simple_df)
        assert res['ok'] is True
        assert res['variables']['x'] == 42
        assert res['variables']['y'] == 'hello'

    def test_callables_excluded_from_variables(self, simple_df):
        res = run_code("def my_fn(): pass", simple_df)
        assert 'my_fn' not in res['variables']

    def test_pd_available_in_scope(self, simple_df):
        res = run_code("df['dt'] = pd.to_datetime('2020-01-01')", simple_df)
        assert res['ok'] is True

    def test_np_available_in_scope(self, simple_df):
        res = run_code("df['log'] = np.log(df['score'])", simple_df)
        assert res['ok'] is True

    def test_extra_ns_injected(self, simple_df):
        res = run_code("df['v'] = extra_val", simple_df, extra_ns={'extra_val': 99})
        assert res['ok'] is True
        assert (res['df']['v'] == 99).all()

    def test_multiline_code(self, simple_df):
        code = (
            "mask = df['score'] >= 85\n"
            "df = df[mask].reset_index(drop=True)\n"
            "df['rank'] = range(1, len(df) + 1)\n"
        )
        res = run_code(code, simple_df)
        assert res['ok'] is True
        assert 'rank' in res['df'].columns
        assert len(res['df']) == 2

    def test_empty_string_code_returns_unchanged_df(self, simple_df):
        res = run_code('', simple_df)
        assert res['ok'] is True
        assert res['df'].equals(simple_df)

    def test_error_is_empty_on_success(self, simple_df):
        res = run_code('pass', simple_df)
        assert res['error'] == ''


# ---------------------------------------------------------------------------
# run_code — error paths
# ---------------------------------------------------------------------------

class TestRunCodeErrors:
    def test_syntax_error(self, simple_df):
        res = run_code('df = df[[[', simple_df)
        assert res['ok'] is False
        assert res['error'] != ''
        assert res['df'] is None

    def test_runtime_name_error(self, simple_df):
        res = run_code('df = undefined_variable', simple_df)
        assert res['ok'] is False
        assert 'NameError' in res['error'] or 'undefined_variable' in res['error']

    def test_df_deleted_returns_error(self, simple_df):
        res = run_code('del df', simple_df)
        assert res['ok'] is False
        assert 'DataFrame' in res['error'] or 'df' in res['error']

    def test_df_replaced_with_non_df_returns_error(self, simple_df):
        res = run_code('df = 42', simple_df)
        assert res['ok'] is False
        assert 'DataFrame' in res['error']

    def test_df_replaced_with_none_returns_error(self, simple_df):
        res = run_code('df = None', simple_df)
        assert res['ok'] is False

    def test_zero_division_error(self, simple_df):
        res = run_code("x = 1 / 0", simple_df)
        assert res['ok'] is False
        assert 'ZeroDivision' in res['error']

    def test_stdout_preserved_on_error(self, simple_df):
        code = "print('before error')\nraise ValueError('boom')"
        res = run_code(code, simple_df)
        assert res['ok'] is False
        assert 'before error' in res['stdout']

    def test_variables_empty_on_error(self, simple_df):
        res = run_code("raise RuntimeError('fail')", simple_df)
        assert res['variables'] == {}

    def test_elapsed_present_on_error(self, simple_df):
        res = run_code("raise RuntimeError('fail')", simple_df)
        assert isinstance(res['elapsed'], float)


# ---------------------------------------------------------------------------
# run_code — edge cases
# ---------------------------------------------------------------------------

class TestRunCodeEdgeCases:
    def test_empty_dataframe(self):
        empty = pd.DataFrame()
        res = run_code('pass', empty)
        assert res['ok'] is True
        assert res['df'].empty

    def test_single_row_dataframe(self):
        df = pd.DataFrame({'a': [1]})
        res = run_code("df['b'] = df['a'] * 2", df)
        assert res['ok'] is True
        assert res['df']['b'].iloc[0] == 2

    def test_wide_dataframe(self):
        df = pd.DataFrame(np.zeros((5, 100)), columns=[f'c{i}' for i in range(100)])
        res = run_code('df = df.fillna(0)', df)
        assert res['ok'] is True
        assert len(res['df'].columns) == 100

    def test_unicode_data(self):
        df = pd.DataFrame({'text': ['héllo', '日本語', 'αβγ']})
        res = run_code("df['upper'] = df['text'].str.upper()", df)
        assert res['ok'] is True

    def test_large_stdout_does_not_crash(self, simple_df):
        res = run_code("for i in range(1000): print(i)", simple_df)
        assert res['ok'] is True
        assert len(res['stdout']) > 0

    def test_internal_df_copy_isolated(self, simple_df):
        """Code that mutates df in-place should not affect the input."""
        res = run_code("df.drop(columns=['score'], inplace=True)", simple_df)
        assert 'score' in simple_df.columns


# ---------------------------------------------------------------------------
# get_package_version
# ---------------------------------------------------------------------------

class TestGetPackageVersion:
    def test_installed_package_returns_string(self):
        ver = get_package_version('pandas')
        assert ver is not None
        assert isinstance(ver, str)

    def test_nonexistent_package_returns_none(self):
        ver = get_package_version('__nonexistent_package_xyz__')
        assert ver is None

    def test_version_format_looks_reasonable(self):
        ver = get_package_version('numpy')
        assert ver is not None
        assert '.' in ver


# ---------------------------------------------------------------------------
# get_sidebar_package_status
# ---------------------------------------------------------------------------

class TestGetSidebarPackageStatus:
    def test_returns_list(self):
        result = get_sidebar_package_status()
        assert isinstance(result, list)

    def test_each_entry_has_required_keys(self):
        for entry in get_sidebar_package_status():
            assert 'name' in entry
            assert 'alias' in entry
            assert 'version' in entry
            assert 'installed' in entry

    def test_pandas_is_installed(self):
        statuses = {e['name']: e for e in get_sidebar_package_status()}
        assert statuses['pandas']['installed'] is True
        assert statuses['pandas']['version'] is not None

    def test_numpy_is_installed(self):
        statuses = {e['name']: e for e in get_sidebar_package_status()}
        assert statuses['numpy']['installed'] is True

    def test_installed_bool_matches_version_presence(self):
        for entry in get_sidebar_package_status():
            if entry['installed']:
                assert entry['version'] is not None
            else:
                assert entry['version'] is None


# ---------------------------------------------------------------------------
# find_replace (transformation_executor)
# ---------------------------------------------------------------------------

class TestFindReplace:
    @pytest.fixture
    def log(self):
        return []

    @pytest.fixture
    def df(self):
        return pd.DataFrame({
            'city': ['New York', 'new york', 'Los Angeles', 'NEW YORK'],
            'code': ['NY', 'ny', 'LA', 'NY'],
        })

    def test_case_insensitive_replaces_all_variants(self, df, log):
        result = find_replace(df, log, 'city', 'new york', 'NYC')
        assert (result['city'] == 'NYC').sum() == 3

    def test_case_sensitive_only_exact_match(self, df, log):
        result = find_replace(df, log, 'city', 'New York', 'NYC', case_sensitive=True)
        assert (result['city'] == 'NYC').sum() == 1
        assert result['city'].iloc[1] == 'new york'

    def test_original_df_not_mutated(self, df, log):
        original = df['city'].tolist()
        find_replace(df, log, 'city', 'New York', 'NYC')
        assert df['city'].tolist() == original

    def test_appends_to_cleaning_log(self, df, log):
        find_replace(df, log, 'city', 'New York', 'NYC')
        assert len(log) == 1
        assert log[0]['action'] == 'find_replace'

    def test_log_records_count(self, df, log):
        find_replace(df, log, 'city', 'new york', 'NYC', case_sensitive=False)
        assert log[0]['values_replaced'] == 3

    def test_no_match_returns_unchanged(self, df, log):
        result = find_replace(df, log, 'city', 'Tokyo', 'T')
        assert result['city'].tolist() == df['city'].tolist()
        assert log[0]['values_replaced'] == 0

    def test_missing_column_raises(self, df, log):
        with pytest.raises(KeyError):
            find_replace(df, log, 'nonexistent', 'x', 'y')

    def test_replace_with_empty_string(self, df, log):
        result = find_replace(df, log, 'code', 'NY', '', case_sensitive=True)
        assert (result['code'] == '').sum() == 2

    def test_string_column_numeric_looking_values(self, log):
        df = pd.DataFrame({'val': ['1', '2', '1', '3']})
        result = find_replace(df, log, 'val', '1', '10')
        assert (result['val'] == '10').sum() == 2

    def test_empty_dataframe(self, log):
        df = pd.DataFrame({'city': pd.Series([], dtype=str)})
        result = find_replace(df, log, 'city', 'x', 'y')
        assert len(result) == 0
        assert log[0]['values_replaced'] == 0


# ---------------------------------------------------------------------------
# st_ace import + signature smoke test
# ---------------------------------------------------------------------------

class TestStAceCompatibility:
    def test_import_succeeds(self):
        from streamlit_ace import st_ace
        assert callable(st_ace)

    def test_signature_has_required_params(self):
        import inspect
        from streamlit_ace import st_ace
        sig = inspect.signature(st_ace)
        params = set(sig.parameters.keys())
        for required in ('value', 'language', 'theme', 'keybinding',
                         'font_size', 'tab_size', 'show_gutter',
                         'show_print_margin', 'wrap', 'auto_update',
                         'height', 'key'):
            assert required in params, f'st_ace missing param: {required}'

    def test_completions_param_absent(self):
        """Ensure the param that caused the TypeError is not present."""
        import inspect
        from streamlit_ace import st_ace
        sig = inspect.signature(st_ace)
        assert 'completions' not in sig.parameters
