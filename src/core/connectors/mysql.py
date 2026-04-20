"""MySQL data source connector."""
import re
import logging
import pandas as pd

from . import DataSourceConnector

logger = logging.getLogger(__name__)

_TABLE_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_.]*$')


class MySQLConnector(DataSourceConnector):
    """Fetch data from a MySQL database via SQLAlchemy."""

    def __init__(self, config: dict):
        """Initialize with source config.

        Config keys:
          - host: database host (default: localhost)
          - port: database port (default: 3306)
          - user: database user
          - password: database password
          - database: database name
          - table_name: (optional) table to query
          - query: (optional) raw SQL SELECT query
          - row_limit: max rows to fetch (default 1 000 000)
        """
        self.config = config

    async def fetch(self) -> pd.DataFrame:
        """Fetch data from MySQL."""
        from src.utils.db_ingestion import build_connection_string, create_connection

        config = self.config
        conn_str = build_connection_string(
            'MySQL',
            host=config.get('host', 'localhost'),
            port=config.get('port', 3306),
            user=config['user'],
            password=config['password'],
            database=config['database'],
        )

        row_limit = int(config.get('row_limit', 1_000_000))
        engine = create_connection(conn_str)

        try:
            if 'table_name' in config:
                table = config['table_name']
                if not _TABLE_RE.match(table):
                    raise ValueError(f"Invalid table name: {table!r}")
                sql = f"SELECT * FROM {table} LIMIT {row_limit}"
            elif 'query' in config:
                inner = config['query']
                sql = f"SELECT * FROM ({inner}) AS _q LIMIT {row_limit}"
            else:
                raise ValueError("MySQLConnector requires 'table_name' or 'query' in config")

            logger.info("Executing MySQL query: %s...", sql[:100])
            df = pd.read_sql(sql, engine)
            logger.info("Fetched %d rows from MySQL", len(df))
            return df

        finally:
            engine.dispose()
