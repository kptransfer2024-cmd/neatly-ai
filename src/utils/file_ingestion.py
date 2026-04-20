"""File parsing utilities for data ingestion."""
import io

import pandas as pd


def _read_csv(f, sep: str = ',') -> pd.DataFrame:
    # Read raw bytes once so every retry uses an in-memory BytesIO — eliminates
    # re-reads from the original file object and avoids seek-position bugs.
    raw = f.read() if hasattr(f, 'read') else open(f, 'rb').read()

    # Fast path 1: PyArrow + UTF-8 (3-10x faster than C engine)
    try:
        return pd.read_csv(io.BytesIO(raw), sep=sep, encoding='utf-8', engine='pyarrow')
    except Exception:
        pass

    # Fast path 2: PyArrow + Latin-1 — handles windows-1252 / ISO-8859 files
    # without falling back to the slow C engine.
    try:
        return pd.read_csv(io.BytesIO(raw), sep=sep, encoding='latin-1', engine='pyarrow')
    except Exception:
        pass

    # Slow path: C engine handles mixed-type columns PyArrow rejects.
    # Try UTF-8 first (correct for well-formed files), then Latin-1 as last resort.
    try:
        return pd.read_csv(io.BytesIO(raw), sep=sep, encoding='utf-8', low_memory=False)
    except Exception:
        return pd.read_csv(io.BytesIO(raw), sep=sep, encoding='latin-1', low_memory=False)


_PARSERS = {
    '.csv': lambda f: _read_csv(f),
    '.tsv': lambda f: _read_csv(f, sep='\t'),
    '.json': lambda f: pd.read_json(f),
    '.xlsx': lambda f: pd.read_excel(f, engine='openpyxl'),
    '.xls': lambda f: pd.read_excel(f, engine='xlrd'),
    '.parquet': lambda f: pd.read_parquet(f),
}


def parse_uploaded_file(file_obj) -> pd.DataFrame:
    """Parse a data file from an uploaded file object.

    Supports CSV, TSV, JSON, Excel (.xlsx, .xls), and Parquet formats.

    Args:
        file_obj: File-like object (e.g., from Streamlit file_uploader)

    Returns:
        Parsed DataFrame

    Raises:
        ValueError: If file format is unsupported
        Exception: If the file cannot be parsed
    """
    ext = _get_ext(file_obj.name)
    parser = _PARSERS.get(ext)
    if parser is None:
        raise ValueError(f"Unsupported file type: '{ext}'. Supported: {', '.join(sorted(_PARSERS.keys()))}")
    return parser(file_obj)


def _get_ext(filename: str) -> str:
    """Extract and normalize file extension."""
    return '.' + filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
