"""S3 connector for fetching CSV/Parquet files from AWS S3."""
import pandas as pd
import logging

from . import DataSourceConnector

logger = logging.getLogger(__name__)


class S3Connector(DataSourceConnector):
    """Connector for AWS S3 CSV and Parquet files."""

    def __init__(self, config: dict):
        """Initialize with source config.

        Config keys:
          - bucket: S3 bucket name
          - key: S3 object key (file path in bucket)
          - aws_access_key_id: (optional, use IAM role if not provided)
          - aws_secret_access_key: (optional)
          - region: (optional, default us-east-1)
        """
        self.config = config

    async def fetch(self) -> pd.DataFrame:
        """Fetch CSV or Parquet from S3."""
        import boto3
        from io import BytesIO
        from src.utils.file_ingestion import parse_uploaded_file

        bucket = self.config.get("bucket")
        key = self.config.get("key")
        if not bucket or not key:
            raise ValueError("S3Connector requires 'bucket' and 'key' in config")

        region = self.config.get("region", "us-east-1")
        logger.info(f"Fetching from S3: s3://{bucket}/{key} (region: {region})")

        # Create S3 client
        s3_kwargs = {
            "region_name": region,
        }
        if "aws_access_key_id" in self.config:
            s3_kwargs["aws_access_key_id"] = self.config["aws_access_key_id"]
            s3_kwargs["aws_secret_access_key"] = self.config.get("aws_secret_access_key")

        s3 = boto3.client("s3", **s3_kwargs)

        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            file_data = response["Body"].read()

            # Create file-like object
            file_obj = BytesIO(file_data)
            file_obj.name = key  # Set filename for extension detection

            df = parse_uploaded_file(file_obj)
            logger.info(f"Fetched {len(df)} rows from S3")
            return df

        except Exception as e:
            logger.error(f"Failed to fetch from S3: {e}")
            raise
