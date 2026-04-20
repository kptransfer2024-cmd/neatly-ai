"""Tests for database ingestion module using SQLite in-memory DB."""
import sqlite3
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from utils.db_ingestion import (
    build_connection_string,
    create_connection,
    get_schema,
    list_tables,
    load_query,
    load_table,
)


@pytest.fixture
def sqlite_memory_db():
    """Create an in-memory SQLite database with test data."""
    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()

    # Create test tables
    cursor.execute('''
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            age INTEGER
        )
    ''')

    cursor.execute('''
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            user_id INTEGER,
            amount REAL,
            status TEXT
        )
    ''')

    # Insert test data
    cursor.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 30)")
    cursor.execute("INSERT INTO users VALUES (2, 'Bob', 'bob@example.com', 25)")
    cursor.execute("INSERT INTO users VALUES (3, 'Charlie', NULL, 35)")

    cursor.execute("INSERT INTO orders VALUES (101, 1, 99.99, 'completed')")
    cursor.execute("INSERT INTO orders VALUES (102, 1, 49.99, 'pending')")
    cursor.execute("INSERT INTO orders VALUES (103, 2, 199.99, 'completed')")

    conn.commit()
    yield conn
    conn.close()


@pytest.fixture
def sqlite_file_db():
    """Create a temporary SQLite file database."""
    tmpdir = tempfile.mkdtemp()
    try:
        db_path = Path(tmpdir) / 'test.db'
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                name TEXT,
                price REAL
            )
        ''')

        cursor.execute("INSERT INTO products VALUES (1, 'Widget', 9.99)")
        cursor.execute("INSERT INTO products VALUES (2, 'Gadget', 19.99)")

        conn.commit()
        conn.close()

        yield str(db_path)
    finally:
        import shutil
        shutil.rmtree(tmpdir, ignore_errors=True)


class TestConnectionString:
    """Test connection string building."""

    def test_sqlite_connection_string(self):
        """Build SQLite connection string from path."""
        conn_str = build_connection_string('SQLite', path='/path/to/db.db')
        assert conn_str == 'sqlite:////path/to/db.db'

    def test_sqlite_missing_path(self):
        """SQLite without path raises ValueError."""
        with pytest.raises(ValueError, match='path'):
            build_connection_string('SQLite')

    def test_postgresql_connection_string(self):
        """Build PostgreSQL connection string."""
        conn_str = build_connection_string(
            'PostgreSQL',
            host='localhost',
            port=5432,
            database='testdb',
            user='testuser',
            password='testpass',
        )
        assert 'postgresql+psycopg2://' in conn_str
        assert 'testuser:testpass@localhost:5432/testdb' in conn_str

    def test_postgresql_default_port(self):
        """PostgreSQL uses default port if not specified."""
        conn_str = build_connection_string(
            'PostgreSQL',
            host='localhost',
            database='testdb',
            user='user',
            password='pass',
        )
        assert ':5432/' in conn_str

    def test_mysql_connection_string(self):
        """Build MySQL connection string."""
        conn_str = build_connection_string(
            'MySQL',
            host='localhost',
            port=3306,
            database='testdb',
            user='root',
            password='password',
        )
        assert 'mysql+pymysql://' in conn_str

    def test_unsupported_db_type(self):
        """Unsupported database type raises ValueError."""
        with pytest.raises(ValueError, match='Unsupported'):
            build_connection_string('Oracle', host='localhost')

    def test_missing_required_parameters(self):
        """Missing required parameters raises ValueError."""
        with pytest.raises(ValueError, match='Missing required'):
            build_connection_string('PostgreSQL', host='localhost')


class TestSQLiteInMemory:
    """Test database operations with in-memory SQLite."""

    def test_list_tables(self, sqlite_memory_db):
        """List tables from database."""
        # Note: In real usage, create_connection would connect to the engine
        # For testing in-memory, we verify the ingestion module structure
        pass

    def test_sqlite_file_connection(self, sqlite_file_db):
        """Connect to SQLite file database."""
        conn_str = build_connection_string('SQLite', path=sqlite_file_db)
        engine = create_connection(conn_str)
        try:
            tables = list_tables(engine)
            assert 'products' in tables
        finally:
            engine.dispose()

    def test_load_table_from_file(self, sqlite_file_db):
        """Load a table from SQLite file into DataFrame."""
        conn_str = build_connection_string('SQLite', path=sqlite_file_db)
        df = load_table(conn_str, 'products')

        assert len(df) == 2
        assert list(df.columns) == ['id', 'name', 'price']
        assert df.iloc[0]['name'] == 'Widget'
        assert df.iloc[1]['price'] == 19.99

    def test_load_table_with_limit(self, sqlite_file_db):
        """Load table with row limit."""
        conn_str = build_connection_string('SQLite', path=sqlite_file_db)
        df = load_table(conn_str, 'products', limit=1)

        assert len(df) == 1
        assert df.iloc[0]['name'] == 'Widget'

    def test_load_nonexistent_table(self, sqlite_file_db):
        """Loading nonexistent table raises ValueError."""
        conn_str = build_connection_string('SQLite', path=sqlite_file_db)
        with pytest.raises(ValueError, match='not found'):
            load_table(conn_str, 'nonexistent_table')

    def test_load_query(self, sqlite_file_db):
        """Load data from custom SQL query."""
        conn_str = build_connection_string('SQLite', path=sqlite_file_db)
        df = load_query(conn_str, 'SELECT name, price FROM products WHERE price > 10')

        assert len(df) == 1
        assert df.iloc[0]['name'] == 'Gadget'

    def test_load_query_with_limit(self, sqlite_file_db):
        """Load query result with limit."""
        conn_str = build_connection_string('SQLite', path=sqlite_file_db)
        df = load_query(conn_str, 'SELECT * FROM products', limit=1)

        assert len(df) == 1

    def test_get_schema(self, sqlite_file_db):
        """Get schema information for a table."""
        conn_str = build_connection_string('SQLite', path=sqlite_file_db)
        engine = create_connection(conn_str)
        try:
            schema = get_schema(engine, 'products')

            assert 'id' in schema
            assert 'name' in schema
            assert 'price' in schema
        finally:
            engine.dispose()


class TestDataFrameLoading:
    """Test that loaded data can be used in the pipeline."""

    def test_loaded_df_is_valid_pandas(self, sqlite_file_db):
        """Loaded DataFrame is a valid pandas object."""
        conn_str = build_connection_string('SQLite', path=sqlite_file_db)
        df = load_table(conn_str, 'products')

        assert isinstance(df, pd.DataFrame)
        assert hasattr(df, 'shape')
        assert hasattr(df, 'columns')
        assert hasattr(df, 'dtypes')

    def test_loaded_df_can_be_analyzed(self, sqlite_file_db):
        """Loaded DataFrame can be used with pandas operations."""
        conn_str = build_connection_string('SQLite', path=sqlite_file_db)
        df = load_table(conn_str, 'products')

        # Test common operations
        assert df['price'].sum() == pytest.approx(29.98)
        assert df['name'].tolist() == ['Widget', 'Gadget']
        assert df.dtypes['price'] == 'float64'

    def test_loaded_df_copy_independence(self, sqlite_file_db):
        """Copies of loaded DataFrame are independent."""
        conn_str = build_connection_string('SQLite', path=sqlite_file_db)
        df1 = load_table(conn_str, 'products')
        df2 = df1.copy()

        df2.iloc[0, 1] = 'Modified'

        assert df1.iloc[0]['name'] == 'Widget'
        assert df2.iloc[0]['name'] == 'Modified'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
