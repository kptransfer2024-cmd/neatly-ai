"""Tests for utils/context_summary.py — deterministic data-context intro."""
import pandas as pd
import pytest

from utils.context_summary import (
    _count_dtypes,
    _infer_theme,
    _salient_columns,
    _tokens,
    summarize_data_context,
)


class TestSummarize:
    def test_empty_frame_no_columns_returns_empty(self):
        assert summarize_data_context(pd.DataFrame()) == ''

    def test_includes_source_name(self):
        df = pd.DataFrame({'a': [1]})
        out = summarize_data_context(df, source_name='sales.csv')
        assert '**sales.csv**' in out

    def test_source_name_optional(self):
        df = pd.DataFrame({'a': [1]})
        out = summarize_data_context(df)
        assert out.startswith('1 rows × 1 columns')

    def test_shape_uses_thousands_separator(self):
        df = pd.DataFrame({'a': range(12345)})
        out = summarize_data_context(df)
        assert '12,345 rows' in out

    def test_dtype_mix_listed(self):
        df = pd.DataFrame({
            'x': [1, 2, 3],
            'y': [1.0, 2.0, 3.0],
            'name': ['a', 'b', 'c'],
            'when': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03']),
            'flag': [True, False, True],
        })
        out = summarize_data_context(df)
        assert '2 numeric' in out
        assert '1 text' in out
        assert '1 date/time' in out
        assert '1 boolean' in out

    def test_theme_inferred_from_multiple_keyword_hits(self):
        df = pd.DataFrame(columns=['customer_id', 'customer_name', 'email', 'phone'])
        out = summarize_data_context(df)
        assert 'customer / contact' in out

    def test_theme_not_inferred_from_single_match(self):
        # Only one keyword hit → below the 2-match threshold
        df = pd.DataFrame(columns=['event_name', 'arbitrary', 'foo'])
        out = summarize_data_context(df)
        assert 'Looks like' not in out

    def test_salient_columns_listed(self):
        df = pd.DataFrame(columns=['id', 'email', 'price', 'misc1', 'misc2'])
        out = summarize_data_context(df)
        assert 'Key columns' in out
        assert '`email`' in out
        assert '`price`' in out

    def test_salient_columns_capped_at_four(self):
        df = pd.DataFrame(columns=['id', 'email', 'price', 'amount', 'total', 'status'])
        out = summarize_data_context(df)
        # Count backticked column names in the "Key columns" line
        key_line = [ln for ln in out.split('\n') if 'Key columns' in ln][0]
        assert key_line.count('`') == 8  # 4 cols × 2 backticks

    def test_no_salient_columns_omits_line(self):
        df = pd.DataFrame(columns=['foo', 'bar', 'baz'])
        out = summarize_data_context(df)
        assert 'Key columns' not in out


class TestCountDtypes:
    def test_numeric_and_text_split(self):
        df = pd.DataFrame({'n': [1, 2], 't': ['a', 'b']})
        counts = _count_dtypes(df)
        assert counts['numeric'] == 1
        assert counts['text'] == 1

    def test_boolean_counted_separately(self):
        df = pd.DataFrame({'b': [True, False], 'n': [1, 2]})
        counts = _count_dtypes(df)
        assert counts['boolean'] == 1
        assert counts['numeric'] == 1

    def test_datetime_counted(self):
        df = pd.DataFrame({'d': pd.to_datetime(['2024-01-01', '2024-02-01'])})
        counts = _count_dtypes(df)
        assert counts['date/time'] == 1


class TestInferTheme:
    def test_returns_none_below_threshold(self):
        assert _infer_theme(['foo', 'bar']) is None

    def test_picks_best_matching_theme(self):
        cols = ['product_id', 'sku', 'stock_level', 'brand']
        assert _infer_theme(cols) == 'product / inventory'

    def test_handles_hyphens_and_spaces(self):
        cols = ['customer-id', 'customer name', 'user email']
        assert _infer_theme(cols) == 'customer / contact'

    def test_unicode_column_names_dont_crash(self):
        assert _infer_theme(['café', 'naïve', 'über']) is None


class TestSalientColumns:
    def test_returns_matching_columns_in_order(self):
        cols = ['misc', 'email', 'price', 'foo', 'total']
        picked = _salient_columns(cols, limit=4)
        assert picked == ['email', 'price', 'total']

    def test_respects_limit(self):
        cols = ['email', 'phone', 'price', 'amount', 'total', 'date']
        assert len(_salient_columns(cols, limit=2)) == 2

    def test_no_matches_returns_empty(self):
        assert _salient_columns(['foo', 'bar', 'baz']) == []


class TestTokens:
    def test_splits_on_underscore(self):
        assert _tokens('customer_id') == {'customer', 'id'}

    def test_splits_on_hyphen(self):
        assert _tokens('order-date') == {'order', 'date'}

    def test_splits_on_space(self):
        assert _tokens('first name') == {'first', 'name'}

    def test_lowercased(self):
        assert _tokens('Customer_ID') == {'customer', 'id'}

    def test_drops_empty_tokens(self):
        # Leading/trailing separators shouldn't produce empty strings
        assert '' not in _tokens('_foo__bar_')
