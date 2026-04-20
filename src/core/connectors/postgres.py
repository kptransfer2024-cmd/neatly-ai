"""PostgreSQL connector for fetching tables and query results."""
import pandas as pd
import logging

from . import DataSourceConnector

logger = logging.getLogger(__name__)


class PostgresConnector(DataSourceConnector):
    """Connector for PostgreSQL databases."""

    def __init__(self, config: dict):
        """Initialize with source config.

        Config keys:
          - connection_string: psycopg2 connection string or SQLAlchemy URL
          - table_name: (optional) table to query
          - query: (optional) SQL SELECT query
          - row_limit: max rows to fetch (default 100000)
        """
        self.config = config

    async def fetch(self) -> pd.DataFrame:
        """Fetch data from PostgreSQL."""
        import asyncpg
        from urllib.parse import urlparse

        connection_string = self.config.get("connection_string")
        if not connection_string:
            raise ValueError("PostgresConnector requires 'connection_string' in config")

        # Parse connection string
        parsed = urlparse(connection_string)
        db_config = {
            "host": parsed.hostname or "localhost",
            "port": parsed.port or 5432,
            "user": parsed.username or "postgres",
            "password": parsed.password or "",
            "database": parsed.path.lstrip("/") if parsed.path else "postgres",
        }

        conn = await asyncpg.connect(**db_config)
        try:
            # Determine which query to run
            if "table_name" in self.config:
                table = self.config["table_name"]
                row_limit = self.config.get("row_limit", 100000)
                query = f"SELECT * FROM {table} LIMIT {row_limit}"
            elif "query" in self.config:
                query = self.config["query"]
                row_limit = self.config.get("row_limit", 100000)
                query += f" LIMIT {row_limit}"
            else:
                raise ValueError("PostgresConnector requires 'table_name' or 'query' in config")

            logger.info(f"Executing PostgreSQL query: {query[:100]}...")
            rows = await conn.fetch(query)

            # Convert to DataFrame
            if not rows:
                return pd.DataFrame()

            df = pd.DataFrame([dict(row) for row in rows])
            logger.info(f"Fetched {len(df)} rows from PostgreSQL")
            return df

        finally:
            await conn.close()
