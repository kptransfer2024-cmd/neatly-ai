"""Database connection and data ingestion utilities."""
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

# SQLAlchemy dialect URL templates for each database type
_DIALECTS = {
    'PostgreSQL': 'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}',
    'MySQL': 'mysql+pymysql://{user}:{password}@{host}:{port}/{database}',
    'MySQL Workbench (Local)': 'mysql+pymysql://{user}:{password}@{host}:{port}/{database}',
    'SQLite': 'sqlite:///{path}',
    'SQL Server': 'mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server',
}

_DEFAULT_PORTS = {
    'PostgreSQL': 5432,
    'MySQL': 3306,
    'MySQL Workbench (Local)': 3306,
    'SQLite': None,
    'SQL Server': 1433,
}


def build_connection_string(db_type: str, **kwargs) -> str:
    """Build SQLAlchemy connection string for the given database type.

    Args:
        db_type: 'PostgreSQL' | 'MySQL' | 'SQLite' | 'SQL Server'
        **kwargs: Connection parameters (host, port, database, user, password, path)

    Returns:
        SQLAlchemy connection URL string

    Raises:
        ValueError: If db_type is unsupported or required parameters are missing
    """
    if db_type not in _DIALECTS:
        raise ValueError(f"Unsupported database type: {db_type}")

    template = _DIALECTS[db_type]

    if db_type == 'SQLite':
        path = kwargs.get('path')
        if not path:
            raise ValueError("SQLite requires 'path' parameter")
        return template.format(path=path)

    # Required for non-SQLite databases
    required = {'host', 'database', 'user', 'password'}
    missing = required - set(kwargs.keys())
    if missing:
        raise ValueError(f"Missing required parameters: {', '.join(missing)}")

    port = kwargs.get('port') or _DEFAULT_PORTS[db_type]
    return template.format(
        host=kwargs['host'],
        port=port,
        database=kwargs['database'],
        user=kwargs['user'],
        password=kwargs['password'],
    )


def create_connection(conn_str: str, timeout: int = 10) -> Engine:
    """Create a SQLAlchemy engine for the connection string.

    Args:
        conn_str: SQLAlchemy connection URL
        timeout: Connection timeout in seconds

    Returns:
        SQLAlchemy Engine object

    Raises:
        Exception: If connection cannot be established
    """
    if conn_str.startswith('sqlite'):
        connect_args = {'timeout': timeout}
    else:
        connect_args = {'connect_timeout': timeout}
    engine = create_engine(conn_str, connect_args=connect_args, echo=False)
    # Test the connection
    with engine.connect() as conn:
        pass
    return engine


def list_tables(engine: Engine) -> list[str]:
    """List all table names in the database.

    Args:
        engine: SQLAlchemy Engine

    Returns:
        List of table names
    """
    inspector = inspect(engine)
    return sorted(inspector.get_table_names())


def load_table(
    conn_str: str,
    table_name: str,
    limit: int = 10000,
) -> pd.DataFrame:
    """Load a table from the database into a DataFrame.

    Args:
        conn_str: SQLAlchemy connection URL
        table_name: Name of the table to load
        limit: Maximum rows to load (prevents OOM on large tables)

    Returns:
        pandas DataFrame

    Raises:
        ValueError: If table doesn't exist
        Exception: If database connection fails
    """
    engine = create_connection(conn_str)

    try:
        # Verify table exists
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        if table_name not in tables:
            raise ValueError(f"Table '{table_name}' not found. Available: {', '.join(tables[:5])}")

        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        df = pd.read_sql(query, engine)
        return df
    finally:
        engine.dispose()


def load_query(
    conn_str: str,
    sql_query: str,
    limit: int = 10000,
) -> pd.DataFrame:
    """Execute a SQL query and return results as a DataFrame.

    Args:
        conn_str: SQLAlchemy connection URL
        sql_query: SQL SELECT query (should not include LIMIT)
        limit: Maximum rows to load

    Returns:
        pandas DataFrame

    Raises:
        Exception: If query execution fails
    """
    engine = create_connection(conn_str)

    try:
        # Wrap in subquery to cap rows without breaking queries that already have LIMIT
        query = f"SELECT * FROM ({sql_query.strip()}) AS _q LIMIT {limit}"

        df = pd.read_sql(query, engine)
        return df
    finally:
        engine.dispose()


def write_table(
    conn_str: str,
    df: pd.DataFrame,
    table_name: str,
    if_exists: str = "append",
    chunk_size: int = 1000,
) -> int:
    """Write a DataFrame to a database table. Returns the number of rows written.

    Args:
        conn_str: SQLAlchemy connection URL
        df: DataFrame to write
        table_name: Destination table name
        if_exists: 'append' | 'replace' | 'fail'
        chunk_size: Rows per INSERT batch

    Returns:
        Number of rows written

    Raises:
        ValueError: If if_exists is invalid
        Exception: If write fails
    """
    if if_exists not in ("append", "replace", "fail"):
        raise ValueError(f"if_exists must be 'append', 'replace', or 'fail'; got {if_exists!r}")

    engine = create_connection(conn_str)
    try:
        df.to_sql(table_name, engine, if_exists=if_exists, index=False, chunksize=chunk_size)
        return len(df)
    finally:
        engine.dispose()


def get_schema(engine: Engine, table_name: str) -> dict:
    """Get column names and types for a table.

    Args:
        engine: SQLAlchemy Engine
        table_name: Name of the table

    Returns:
        Dict of {column_name: column_type}

    Raises:
        ValueError: If table doesn't exist
    """
    inspector = inspect(engine)
    if table_name not in inspector.get_table_names():
        raise ValueError(f"Table '{table_name}' not found")

    columns = inspector.get_columns(table_name)
    return {col['name']: str(col['type']) for col in columns}
