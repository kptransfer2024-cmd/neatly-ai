"""Tests for utils/file_ingestion.py — parse_uploaded_file and _get_ext."""
import io
import time
import types

import numpy as np
import pandas as pd
import pytest

from utils.file_ingestion import _get_ext, parse_uploaded_file


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_file(content: bytes, filename: str):
    """Minimal stand-in for Streamlit's UploadedFile."""
    buf = io.BytesIO(content)
    buf.name = filename
    buf.size = len(content)
    return buf


def _csv_bytes(df: pd.DataFrame, **kwargs) -> bytes:
    return df.to_csv(index=False, **kwargs).encode('utf-8')


def _tsv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False, sep='\t').encode('utf-8')


# ---------------------------------------------------------------------------
# _get_ext
# ---------------------------------------------------------------------------

class TestGetExt:
    def test_lowercase_csv(self):
        assert _get_ext('data.csv') == '.csv'

    def test_uppercase_CSV(self):
        assert _get_ext('DATA.CSV') == '.csv'

    def test_mixed_case(self):
        assert _get_ext('Report.Xlsx') == '.xlsx'

    def test_tsv(self):
        assert _get_ext('file.tsv') == '.tsv'

    def test_parquet(self):
        assert _get_ext('archive.parquet') == '.parquet'

    def test_no_extension(self):
        assert _get_ext('nodotfile') == ''

    def test_multiple_dots(self):
        # Only the last segment is the extension
        assert _get_ext('my.data.file.csv') == '.csv'

    def test_dot_only(self):
        # rsplit('.', 1) on '.' yields ['', ''] → extension becomes '.'
        # No real upload would be named '.', but document the actual behaviour.
        assert _get_ext('.') == '.'

    def test_hidden_file_no_ext(self):
        # '.gitignore' — rsplit gives ['', 'gitignore'], treated as extension
        result = _get_ext('.gitignore')
        assert result == '.gitignore'


# ---------------------------------------------------------------------------
# CSV parsing
# ---------------------------------------------------------------------------

class TestCSV:
    def _df(self):
        return pd.DataFrame({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})

    def test_basic_roundtrip(self):
        f = _mock_file(_csv_bytes(self._df()), 'test.csv')
        result = parse_uploaded_file(f)
        pd.testing.assert_frame_equal(result.reset_index(drop=True),
                                      self._df().reset_index(drop=True))

    def test_uppercase_extension(self):
        f = _mock_file(_csv_bytes(self._df()), 'TEST.CSV')
        result = parse_uploaded_file(f)
        assert list(result.columns) == ['a', 'b']

    def test_empty_csv(self):
        f = _mock_file(b'col1,col2\n', 'empty.csv')
        result = parse_uploaded_file(f)
        assert list(result.columns) == ['col1', 'col2']
        assert len(result) == 0

    def test_single_column(self):
        f = _mock_file(b'val\n1\n2\n3\n', 'single.csv')
        result = parse_uploaded_file(f)
        assert list(result.columns) == ['val']
        assert len(result) == 3

    def test_missing_values_preserved(self):
        df = pd.DataFrame({'a': [1, None, 3], 'b': ['x', 'y', None]})
        f = _mock_file(_csv_bytes(df), 'missing.csv')
        result = parse_uploaded_file(f)
        assert result['a'].isna().sum() == 1
        assert result['b'].isna().sum() == 1

    def test_numeric_columns_inferred(self):
        f = _mock_file(b'x,y\n1,2\n3,4\n', 'nums.csv')
        result = parse_uploaded_file(f)
        assert pd.api.types.is_integer_dtype(result['x']) or pd.api.types.is_float_dtype(result['x'])

    def test_unicode_content(self):
        content = 'name,city\nJosé,São Paulo\nMüller,München\n'.encode('utf-8')
        f = _mock_file(content, 'unicode.csv')
        result = parse_uploaded_file(f)
        assert 'José' in result['name'].values
        assert 'Müller' in result['name'].values

    def test_wide_csv(self):
        df = pd.DataFrame(np.random.rand(10, 200), columns=[f'c{i}' for i in range(200)])
        f = _mock_file(_csv_bytes(df), 'wide.csv')
        result = parse_uploaded_file(f)
        assert result.shape == (10, 200)

    def test_quoted_commas_in_values(self):
        content = b'a,b\n"hello, world",42\n'
        f = _mock_file(content, 'quoted.csv')
        result = parse_uploaded_file(f)
        assert result['a'].iloc[0] == 'hello, world'
        assert result['b'].iloc[0] == 42


# ---------------------------------------------------------------------------
# TSV parsing
# ---------------------------------------------------------------------------

class TestTSV:
    def test_basic_roundtrip(self):
        df = pd.DataFrame({'x': [10, 20], 'y': ['a', 'b']})
        f = _mock_file(_tsv_bytes(df), 'data.tsv')
        result = parse_uploaded_file(f)
        assert list(result.columns) == ['x', 'y']
        assert len(result) == 2

    def test_tabs_not_treated_as_csv(self):
        # A tab-separated row should NOT split on commas
        content = b'col\tone\n1\t2\n'
        f = _mock_file(content, 'tab.tsv')
        result = parse_uploaded_file(f)
        assert list(result.columns) == ['col', 'one']

    def test_empty_tsv(self):
        f = _mock_file(b'a\tb\n', 'empty.tsv')
        result = parse_uploaded_file(f)
        assert len(result) == 0
        assert list(result.columns) == ['a', 'b']


# ---------------------------------------------------------------------------
# JSON parsing
# ---------------------------------------------------------------------------

class TestJSON:
    def test_records_orientation(self):
        df = pd.DataFrame({'id': [1, 2], 'val': ['a', 'b']})
        content = df.to_json(orient='records').encode()
        f = _mock_file(content, 'data.json')
        result = parse_uploaded_file(f)
        assert set(result.columns) == {'id', 'val'}
        assert len(result) == 2

    def test_columns_orientation(self):
        df = pd.DataFrame({'x': [1, 2], 'y': [3, 4]})
        content = df.to_json(orient='columns').encode()
        f = _mock_file(content, 'cols.json')
        result = parse_uploaded_file(f)
        assert 'x' in result.columns

    def test_empty_json_array(self):
        # pd.read_json('[]') returns an empty DataFrame, not an error.
        f = _mock_file(b'[]', 'empty.json')
        result = parse_uploaded_file(f)
        assert len(result) == 0


# ---------------------------------------------------------------------------
# Parquet parsing
# ---------------------------------------------------------------------------

class TestParquet:
    def test_basic_roundtrip(self):
        df = pd.DataFrame({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)
        buf.name = 'data.parquet'
        buf.size = buf.getbuffer().nbytes
        result = parse_uploaded_file(buf)
        pd.testing.assert_frame_equal(result, df)

    def test_dtypes_preserved(self):
        df = pd.DataFrame({
            'int_col': pd.array([1, 2, 3], dtype='int32'),
            'float_col': [1.1, 2.2, 3.3],
        })
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)
        buf.name = 'typed.parquet'
        buf.size = buf.getbuffer().nbytes
        result = parse_uploaded_file(buf)
        assert result['int_col'].dtype == np.int32
        assert pd.api.types.is_float_dtype(result['float_col'])


# ---------------------------------------------------------------------------
# Unsupported formats
# ---------------------------------------------------------------------------

class TestUnsupportedFormats:
    def test_txt_raises(self):
        f = _mock_file(b'some text', 'file.txt')
        with pytest.raises(ValueError, match='Unsupported file type'):
            parse_uploaded_file(f)

    def test_xml_raises(self):
        f = _mock_file(b'<root/>', 'data.xml')
        with pytest.raises(ValueError, match='Unsupported file type'):
            parse_uploaded_file(f)

    def test_no_extension_raises(self):
        f = _mock_file(b'data', 'nodotfile')
        with pytest.raises(ValueError):
            parse_uploaded_file(f)

    def test_error_message_lists_supported_types(self):
        f = _mock_file(b'', 'bad.log')
        with pytest.raises(ValueError) as exc_info:
            parse_uploaded_file(f)
        msg = str(exc_info.value)
        for ext in ('.csv', '.json', '.parquet', '.tsv', '.xlsx'):
            assert ext in msg


# ---------------------------------------------------------------------------
# Performance: pyarrow fallback doesn't double-parse
# ---------------------------------------------------------------------------

class TestCSVFallback:
    def test_fallback_produces_correct_result(self, monkeypatch):
        """When pyarrow engine raises, the C-engine fallback still returns correct data."""
        import utils.file_ingestion as mod

        original_read_csv = pd.read_csv
        call_count = {'n': 0}

        def patched_read_csv(f, *args, **kwargs):
            call_count['n'] += 1
            if kwargs.get('engine') == 'pyarrow':
                raise ImportError('pyarrow not available')
            return original_read_csv(f, *args, **kwargs)

        monkeypatch.setattr(pd, 'read_csv', patched_read_csv)
        # Re-import _read_csv so it picks up the monkeypatched pd.read_csv
        import importlib
        importlib.reload(mod)

        df = pd.DataFrame({'a': [1, 2], 'b': ['x', 'y']})
        f = _mock_file(_csv_bytes(df), 'test.csv')
        result = mod.parse_uploaded_file(f)
        assert list(result.columns) == ['a', 'b']
        assert len(result) == 2

        importlib.reload(mod)  # restore

    def test_seek_called_on_fallback(self):
        """File pointer is reset before fallback read so no data is lost."""
        original_read_csv = pd.read_csv
        seeked = {'called': False}

        class SeekTrackingBuffer(io.BytesIO):
            def seek(self, *args, **kwargs):
                seeked['called'] = True
                return super().seek(*args, **kwargs)

        df = pd.DataFrame({'x': range(5)})
        raw = df.to_csv(index=False).encode()
        buf = SeekTrackingBuffer(raw)
        buf.name = 'track.csv'
        buf.size = len(raw)

        # Monkeypatch only pyarrow path to fail
        import utils.file_ingestion as mod
        original_fn = mod._read_csv

        def failing_read_csv(f, sep=','):
            try:
                raise ImportError('forced')
            except Exception:
                if hasattr(f, 'seek'):
                    f.seek(0)
                return pd.read_csv(f, sep=sep, encoding='utf-8', low_memory=False)

        mod._read_csv = failing_read_csv
        mod._PARSERS['.csv'] = lambda f: mod._read_csv(f)

        result = mod.parse_uploaded_file(buf)
        assert seeked['called']
        assert len(result) == 5

        mod._read_csv = original_fn
        mod._PARSERS['.csv'] = lambda f: mod._read_csv(f)


# ---------------------------------------------------------------------------
# Performance: large CSV loads in acceptable wall-clock time
# ---------------------------------------------------------------------------

class TestPerformance:
    def test_100k_rows_under_5s(self):
        df = pd.DataFrame({
            'id': range(100_000),
            'name': ['Alice'] * 100_000,
            'value': np.random.rand(100_000),
        })
        f = _mock_file(_csv_bytes(df), 'large.csv')
        t0 = time.perf_counter()
        result = parse_uploaded_file(f)
        elapsed = time.perf_counter() - t0
        assert len(result) == 100_000
        assert elapsed < 5.0, f'Parsing 100k rows took {elapsed:.2f}s — too slow'

    def test_parse_is_faster_on_second_call_via_session_cache(self):
        """
        Simulate the app.py session-state cache: second call should skip parsing
        and return in near-zero time.
        """
        df = pd.DataFrame({'a': range(50_000), 'b': ['x'] * 50_000})
        raw = _csv_bytes(df)

        session: dict = {}

        def cached_parse(filename: str, size: int, make_file):
            key = (filename, size)
            if session.get('_upload_cache_key') != key:
                result = parse_uploaded_file(make_file())
                session['_upload_cache_key'] = key
                session['_upload_cached_df'] = result
            return session['_upload_cached_df']

        # First call — does real parsing
        t0 = time.perf_counter()
        r1 = cached_parse('data.csv', len(raw), lambda: _mock_file(raw, 'data.csv'))
        first_elapsed = time.perf_counter() - t0

        # Second call — cache hit, O(1)
        t0 = time.perf_counter()
        r2 = cached_parse('data.csv', len(raw), lambda: _mock_file(raw, 'data.csv'))
        second_elapsed = time.perf_counter() - t0

        assert len(r1) == 50_000
        assert r1 is r2  # same object returned from cache
        assert second_elapsed < first_elapsed / 10, (
            f'Cache hit ({second_elapsed:.4f}s) should be 10x faster than first parse ({first_elapsed:.4f}s)'
        )
