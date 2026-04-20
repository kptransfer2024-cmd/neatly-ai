"""Upload connector for local files and S3."""
import pandas as pd
import logging

from . import DataSourceConnector

logger = logging.getLogger(__name__)


class UploadConnector(DataSourceConnector):
    """Connector for uploaded files (local path or S3)."""

    def __init__(self, config: dict):
        """Initialize with source config.

        Config keys:
          - path: local file path (for local development)
          - bucket: S3 bucket name (for production)
          - key: S3 object key
        """
        self.config = config

    async def fetch(self) -> pd.DataFrame:
        """Fetch data from local path or S3."""
        if "path" in self.config:
            return self._fetch_local()
        elif "bucket" in self.config:
            return await self._fetch_s3()
        else:
            raise ValueError("UploadConnector requires 'path' or 'bucket' in config")

    def _fetch_local(self) -> pd.DataFrame:
        """Load from local file path."""
        from src.utils.file_ingestion import parse_uploaded_file

        path = self.config["path"]
        logger.info(f"Loading file from local path: {path}")

        # For local files, we need a file-like object
        # This is a simplified version; in production, use a real file object
        with open(path, "rb") as f:
            return parse_uploaded_file(f)

    async def _fetch_s3(self) -> pd.DataFrame:
        """Load from S3."""
        import boto3
        from io import BytesIO
        from src.utils.file_ingestion import parse_uploaded_file

        bucket = self.config["bucket"]
        key = self.config["key"]
        logger.info(f"Loading file from S3: s3://{bucket}/{key}")

        s3 = boto3.client("s3")
        response = s3.get_object(Bucket=bucket, Key=key)

        # Create file-like object from S3 response
        file_obj = BytesIO(response["Body"].read())
        file_obj.name = key  # Set filename for extension detection

        return parse_uploaded_file(file_obj)
