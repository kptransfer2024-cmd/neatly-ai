"""Data source connectors for fetching data from various sources."""
from abc import ABC, abstractmethod
import pandas as pd


class DataSourceConnector(ABC):
    """Base class for data source connectors."""

    @abstractmethod
    async def fetch(self) -> pd.DataFrame:
        """Fetch data from the source and return a DataFrame."""
        pass


def get_connector(source_type: str, source_config: dict) -> DataSourceConnector:
    """Factory function to create a connector by type."""
    if source_type == "upload":
        from .upload import UploadConnector
        return UploadConnector(source_config)
    elif source_type == "postgres":
        from .postgres import PostgresConnector
        return PostgresConnector(source_config)
    elif source_type == "s3":
        from .s3 import S3Connector
        return S3Connector(source_config)
    elif source_type == "bigquery":
        from .bigquery import BigQueryConnector
        return BigQueryConnector(source_config)
    else:
        raise ValueError(f"Unknown source type: {source_type}")


__all__ = ["DataSourceConnector", "get_connector"]
