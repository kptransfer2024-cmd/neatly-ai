"""File parsing utilities for data ingestion."""
import pandas as pd


def parse_uploaded_file(file_obj, encoding: str = 'utf-8') -> pd.DataFrame:
    """Parse a CSV file from an uploaded file object.

    Args:
        file_obj: File-like object (e.g., from Streamlit file_uploader or BytesIO)
        encoding: Character encoding for the CSV

    Returns:
        Parsed DataFrame

    Raises:
        Exception: If the file cannot be parsed as CSV
    """
    return pd.read_csv(file_obj, encoding=encoding)
