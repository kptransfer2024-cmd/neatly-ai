"""File parsing utilities for data ingestion."""
import pandas as pd

_PARSERS = {
    '.csv': lambda f: pd.read_csv(f, encoding='utf-8'),
    '.tsv': lambda f: pd.read_csv(f, sep='\t', encoding='utf-8'),
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
