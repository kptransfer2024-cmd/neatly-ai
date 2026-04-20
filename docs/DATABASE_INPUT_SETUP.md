# Database Input Sources Setup Guide

This guide explains how to set up and use the new database input feature in Neatly AI, and how to configure MCP servers for Claude Code integration.

---

## Part 1: Using Database Input in Streamlit App

The app now supports loading data directly from databases via a new **Database** tab in the upload stage.

### Supported Databases

- **PostgreSQL** (via psycopg2)
- **MySQL** (via PyMySQL)
- **SQLite** (file or in-memory)
- **SQL Server** (via ODBC)

### User Workflow

1. Start the Streamlit app: `streamlit run app.py`
2. In the upload stage, click the **🗄️ Database** tab
3. Select database type from the dropdown
4. Enter connection credentials:
   - **For PostgreSQL/MySQL/SQL Server:** host, port, database, username, password
   - **For SQLite:** just the file path
5. Choose how to load data:
   - **Select Table:** Pick a table from the dropdown
   - **Custom Query:** Write a SQL SELECT statement
6. Set row limit (default 10,000 to prevent OOM on large tables)
7. Click "Connect & Load" to preview the data
8. Click "Start Diagnosis" to enter the cleaning pipeline

The loaded DataFrame flows through the same cleaning, explanation, and export stages as file uploads.

---

## Part 2: Dependencies

Install required packages:

```bash
pip install -r requirements.txt
```

This installs:
- `sqlalchemy>=2.0.0` — database abstraction layer
- `psycopg2-binary>=2.9.0` — PostgreSQL driver
- `pymysql>=1.1.0` — MySQL driver

For SQL Server (optional), also install:
```bash
pip install pyodbc
# On Windows: usually pre-installed
# On Linux/macOS: apt-get install unixodbc-dev (or equivalent)
```

---

## Part 3: Code Structure

### New Module: `utils/db_ingestion.py`

Mirrors the design of `utils/file_ingestion.py`:

```python
# Build connection strings
build_connection_string(db_type, **kwargs) -> str

# Create a database engine
create_connection(conn_str, timeout=10) -> Engine

# List tables
list_tables(engine) -> list[str]

# Load data
load_table(conn_str, table_name, limit=10000) -> pd.DataFrame
load_query(conn_str, sql_query, limit=10000) -> pd.DataFrame

# Schema info
get_schema(engine, table_name) -> dict
```

### Integration: `app.py`

Added functions:
- `_render_database_loader()` — UI for database connection
- `_finalize_database_load()` — Show preview and start diagnosis
- `_get_default_port()` — Default ports for each DB type

---

## Part 4: MCP Server Integration (For Claude Code)

For Claude Code developers, you can query databases directly using MCP servers. This is useful for:
- Seeding test data
- Loading real data for development
- Debugging data issues without opening the Streamlit app

### Option A: Using mcp-alchemy (Recommended)

`mcp-alchemy` supports all major databases with a single install.

**Install:**
```bash
pip install mcp-alchemy uvx
```

**Configure in `.claude/settings.local.json`:**

```json
{
  "mcpServers": {
    "mcp-alchemy": {
      "command": "uvx",
      "args": ["mcp-alchemy"],
      "env": {
        "DB_URL": "postgresql+psycopg2://user:password@localhost:5432/mydb"
      }
    }
  }
}
```

**Swap `DB_URL` for your database:**

- **PostgreSQL:** `postgresql+psycopg2://user:pass@host:5432/db`
- **MySQL:** `mysql+pymysql://user:pass@host:3306/db`
- **SQLite:** `sqlite:///path/to/file.db`

**Available MCP tools:**
- `all_table_names` — List all tables
- `filter_table_names` — Search for tables by name
- `schema_definitions` — Get column names and types
- `query` — Execute SELECT queries and get results

**Example: Query from Claude Code**

Ask Claude Code:

```
List the tables in my database and show me the schema for the users table.
```

Claude Code will use MCP tools to:
1. Call `all_table_names` → get list of tables
2. Call `schema_definitions` with `users` → get columns and types
3. Return the results

---

### Option B: Using Official PostgreSQL Server

If you only use PostgreSQL:

```json
{
  "mcpServers": {
    "postgres": {
      "command": "npx",
      "args": ["@modelcontextprotocol/server-postgres"],
      "env": {
        "PG_CONNECTION_STRING": "postgresql://user:password@localhost:5432/mydb"
      }
    }
  }
}
```

---

## Part 5: Testing

Run database ingestion tests:

```bash
# All database tests
pytest tests/test_db_ingestion.py -v

# Specific test
pytest tests/test_db_ingestion.py::TestSQLiteInMemory::test_load_table_from_file -v
```

Tests use **SQLite in-memory databases**, so no external database server is required.

---

## Part 6: Security Notes

### Credentials Handling

- Connection credentials are **never logged or persisted**
- They stay in Python memory during the session only
- They are **never sent to the Claude API**
- No database passwords appear in session state

### Connection Timeout

- Default timeout: 10 seconds
- Prevents hanging connections
- Configurable in `create_connection()`

### SQL Injection Prevention

- Uses SQLAlchemy's parameterized queries
- Never constructs SQL strings manually
- Safe for user-provided SQL queries (with query limits)

---

## Part 7: Troubleshooting

### "No tables found in database"
- Database exists but is empty
- User doesn't have permission to read table list
- Try connecting as an admin user

### "Connection refused"
- Database server not running
- Wrong host/port
- Firewall blocking connection

### "Module not found: sqlalchemy"
- Run: `pip install -r requirements.txt`

### "PermissionError: database is locked" (SQLite)
- Another process has the database open
- Close the file in other applications
- Check for `.db-wal` and `.db-shm` lock files

### "Row limit exceeded"
- Set a higher row limit slider (up to 100,000)
- Or use a custom query with WHERE clause to reduce results

---

## Part 8: Examples

### Load PostgreSQL table

```
Database Type: PostgreSQL
Host: localhost
Port: 5432
Database: analytics
Username: analyst
Password: ***
Load Mode: Select Table
Table: customers
Row Limit: 50,000
```

### Load MySQL with custom query

```
Database Type: MySQL
Host: db.example.com
Port: 3306
Database: sales
Username: user
Password: ***
Load Mode: Custom Query
SQL Query: SELECT * FROM orders WHERE created_at > '2024-01-01'
Row Limit: 10,000
```

### Load SQLite file

```
Database Type: SQLite
Database File Path: /home/user/data/sample.db
Load Mode: Select Table
Table: transactions
```

---

## Maintenance

### Updating MCP Server

```bash
# Update mcp-alchemy
pip install --upgrade mcp-alchemy

# Reload Claude Code to pick up new version
```

### Clearing SQLAlchemy connection pool

If connections seem stale, the engines dispose themselves automatically. No manual cleanup needed.

---

## Related Files

- `utils/db_ingestion.py` — Database loading logic
- `app.py` — UI integration (lines 30-35, 89-193)
- `requirements.txt` — Database dependencies
- `tests/test_db_ingestion.py` — Test suite
