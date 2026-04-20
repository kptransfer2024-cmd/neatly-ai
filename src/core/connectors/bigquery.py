"""BigQuery connector (placeholder for Phase 3)."""
import pandas as pd
import logging

from . import DataSourceConnector

logger = logging.getLogger(__name__)


class BigQueryConnector(DataSourceConnector):
    """Connector for Google BigQuery tables."""

    def __init__(self, config: dict):
        """Initialize with source config.

        Config keys:
          - project_id: GCP project ID
          - dataset: BigQuery dataset name
          - table: table name
          - query: (optional) custom SQL query
          - credentials_json: (optional) path to service account JSON
        """
        self.config = config

    async def fetch(self) -> pd.DataFrame:
        """Fetch data from BigQuery (placeholder)."""
        raise NotImplementedError(
            "BigQuery connector coming in Phase 3. "
            "Use PostgreSQL or S3 connectors for now."
        )
