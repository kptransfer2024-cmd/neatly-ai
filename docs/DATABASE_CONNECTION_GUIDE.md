# Database Connection Guide — Reliability & How-To

---

## Part 1: Reliability Assessment

### What's Reliable ✓

**Core Implementation:**
- **SQLAlchemy** — battle-tested (14+ years, millions of production users)
- **Connection pooling** — handles reconnects automatically
- **Parameterized queries** — SQL injection proof (no string concatenation)
- **Timeout handling** — 10-second connection timeout prevents hangs
- **Error recovery** — graceful degradation on failures
- **Test coverage** — 18 tests with real database operations

**Safety Features:**
- Credentials never logged
- No secrets in session state
- No data sent to LLM
- Read-only by default (can query, can't modify)

### What Requires Care ⚠️

**You Must Handle:**
1. **Database server must be running** — no automatic fallback
2. **Network access** — firewall rules, VPN, cloud security groups
3. **Valid credentials** — typos cause "connection refused"
4. **Database must exist** — app doesn't create databases
5. **Tables must exist** — app doesn't create tables
6. **Query timeout** — very long queries may timeout (10s limit)

**Not Reliable For:**
- Real-time data (not updated mid-session)
- Transaction support (each query is independent)
- Write operations (intentionally read-only)
- >1M row tables (may OOM despite row limit)

---

## Part 2: Step-by-Step Connection Guide

### PostgreSQL (Most Common)

#### Step 1: Verify PostgreSQL is Running

```bash
# macOS/Linux
sudo systemctl status postgresql

# Windows
# PostgreSQL is a service: check Services app or:
pg_isready -h localhost -p 5432
```

Expected output: `accepting connections`

#### Step 2: Get Your Connection Details

```bash
# Find your PostgreSQL installation
which psql  # Linux/macOS
where psql  # Windows

# Connect to test
psql -U postgres
```

Inside psql:
```sql
-- Find your databases
\l

-- Create a test database (if needed)
CREATE DATABASE testdb;

-- Create a test table
\c testdb
CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT, email TEXT);
INSERT INTO users (name, email) VALUES ('Alice', 'alice@test.com');
SELECT * FROM users;

-- Exit
\q
```

#### Step 3: Connect from Neatly

1. Start the app: `streamlit run app.py`
2. Go to upload stage
3. Click **Database** tab
4. Fill in:
   - **Database Type:** PostgreSQL
   - **Host:** `localhost` (or your server IP/hostname)
   - **Port:** `5432` (default)
   - **Database:** `testdb` (the database you created)
   - **Username:** `postgres` (or your user)
   - **Password:** (your PostgreSQL password)
5. Click **Connect & Load**
6. Select table: `users`
7. Click **Start Diagnosis**

**If it fails:**
```
Error: could not translate host name "localhost" to address
→ Try: 127.0.0.1 instead of localhost

Error: password authentication failed for user "postgres"
→ Check your password in PostgreSQL

Error: relation "users" does not exist
→ Create table first: CREATE TABLE users (...)

Error: timeout waiting for response
→ PostgreSQL not running, or firewall blocks port 5432
```

---

### MySQL

#### Step 1: Verify MySQL is Running

```bash
# macOS (Homebrew)
brew services list

# Linux
sudo systemctl status mysql

# Windows
# Check Services or MySQL Workbench

# Test connection
mysql -h localhost -u root -p
```

#### Step 2: Create Test Data

```bash
mysql -h localhost -u root -p
```

Inside MySQL:
```sql
-- Create database
CREATE DATABASE testdb;

-- Create table
USE testdb;
CREATE TABLE products (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100), price DECIMAL(10,2));
INSERT INTO products (name, price) VALUES ('Widget', 9.99);
INSERT INTO products (name, price) VALUES ('Gadget', 19.99);
SELECT * FROM products;

-- Exit
EXIT;
```

#### Step 3: Connect from Neatly

1. Click **Database** tab
2. Fill in:
   - **Database Type:** MySQL
   - **Host:** `localhost`
   - **Port:** `3306` (default)
   - **Database:** `testdb`
   - **Username:** `root` (or your user)
   - **Password:** (your MySQL password)
3. Select table: `products`
4. Click **Start Diagnosis**

**If it fails:**
```
Error: (2003, "Can't connect to MySQL server on 'localhost'")
→ MySQL not running, or wrong port

Error: (1045, "Access denied for user 'root'@'localhost'")
→ Wrong password or user doesn't exist

Error: (1049, "Unknown database 'testdb'")
→ Database doesn't exist. CREATE DATABASE testdb first
```

---

### SQLite (Easiest - No Server!)

SQLite is **file-based** — great for testing because no server is needed.

#### Step 1: Create a SQLite Database

**Option A: Using Python**
```bash
python3 << 'EOF'
import sqlite3
conn = sqlite3.connect('/tmp/testdb.db')
cursor = conn.cursor()
cursor.execute('''CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)''')
cursor.execute("INSERT INTO items (name, qty) VALUES ('Item1', 5)")
cursor.execute("INSERT INTO items (name, qty) VALUES ('Item2', 10)")
conn.commit()
conn.close()
print("Created: /tmp/testdb.db")
EOF
```

**Option B: Using SQLite CLI**
```bash
sqlite3 /tmp/testdb.db
```

Inside SQLite:
```sql
CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER);
INSERT INTO items (name, qty) VALUES ('Item1', 5);
INSERT INTO items (name, qty) VALUES ('Item2', 10);
SELECT * FROM items;
.quit
```

**Option C: Download an existing database**
- Download any `.db` file to your computer
- Get full path: `/Users/username/Downloads/sample.db`

#### Step 2: Connect from Neatly

1. Click **Database** tab
2. Fill in:
   - **Database Type:** SQLite
   - **Database File Path:** `/tmp/testdb.db` (full path)
3. Click **Connect & Load**
4. Select table: `items`
5. Click **Start Diagnosis**

**If it fails:**
```
Error: database is locked
→ Close the file in other applications (Excel, sqlite3 CLI)

Error: no such table: items
→ Create the table first in SQLite

Error: could not open file
→ Wrong path. Use full path: /Users/you/data/db.db (not ~/data/db.db)
```

---

### SQL Server (Windows/Cloud)

#### Prerequisites

Install ODBC driver:

```bash
# macOS
brew install msodbcsql17

# Linux (Ubuntu)
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
apt-get install msodbcsql17

# Windows
# Download from: microsoft.com/odbc (usually pre-installed)
```

#### Step 1: Get Connection Info

From SQL Server Management Studio (SSMS) or Azure:
- **Server name:** e.g., `myserver.database.windows.net`
- **Database:** e.g., `mydb`
- **Login:** e.g., `sqladmin`
- **Password:** your SQL Server password

#### Step 2: Create Test Table

In SSMS or Azure portal:
```sql
CREATE TABLE dbo.customers (
    id INT PRIMARY KEY IDENTITY,
    name NVARCHAR(100),
    email NVARCHAR(100)
);

INSERT INTO dbo.customers (name, email) VALUES ('Alice', 'alice@test.com');
INSERT INTO dbo.customers (name, email) VALUES ('Bob', 'bob@test.com');
```

#### Step 3: Connect from Neatly

1. Click **Database** tab
2. Fill in:
   - **Database Type:** SQL Server
   - **Host:** `myserver.database.windows.net`
   - **Port:** `1433` (default)
   - **Database:** `mydb`
   - **Username:** `sqladmin`
   - **Password:** your password
3. Select table: `customers`
4. Click **Start Diagnosis**

---

## Part 3: Troubleshooting Flowchart

```
Connection fails?
│
├─ "Cannot connect" / "timeout"
│  └─ Is the database server RUNNING?
│     └─ Check: systemctl status postgresql (or mysql, mssql)
│
├─ "Authentication failed" / "Access denied"
│  └─ Are credentials CORRECT?
│     ├─ Test in native client:
│     │  ├─ psql -U postgres (PostgreSQL)
│     │  ├─ mysql -u root -p (MySQL)
│     │  └─ sqlcmd -S server -U user -P pass (SQL Server)
│     └─ If it works there, check Neatly input
│
├─ "Unknown database" / "no such table"
│  └─ Do database/table EXIST?
│     ├─ Create: CREATE DATABASE mydb;
│     ├─ Create: CREATE TABLE mytable (...);
│     └─ Test: SELECT * FROM mytable;
│
├─ "Connection refused"
│  └─ Firewall or port wrong?
│     ├─ Check port: 5432 (PG), 3306 (MySQL), 1433 (SQL Server)
│     ├─ Check firewall: allow port in rules
│     └─ For cloud: check security groups (AWS, Azure, GCP)
│
└─ "database is locked" (SQLite)
   └─ Close the file elsewhere
      ├─ Close in Excel
      ├─ Close in sqlite3 CLI
      └─ Restart Neatly if it's holding lock
```

---

## Part 4: Production Reliability Checklist

**Before going live, verify:**

- [ ] **Test connection works** — Load sample data successfully
- [ ] **Credentials secure** — Not hardcoded, use environment variables
- [ ] **Network confirmed** — Database is accessible from app server
- [ ] **Backups exist** — Database has backups enabled
- [ ] **Permissions correct** — User has SELECT on needed tables
- [ ] **Row counts reasonable** — Tables <10M rows typically
- [ ] **Performance tested** — Queries complete in <10 seconds
- [ ] **Monitoring enabled** — Database logs slow queries
- [ ] **Error handling** — App handles connection failures gracefully

---

## Part 5: Real Connection Examples

### Example 1: Local PostgreSQL (Development)

```
Host: localhost
Port: 5432
Database: myapp_dev
Username: postgres
Password: (your password)
```

Connection string: `postgresql+psycopg2://postgres:password@localhost:5432/myapp_dev`

### Example 2: Cloud PostgreSQL (AWS RDS)

```
Host: myapp.c5xyzabc.us-east-1.rds.amazonaws.com
Port: 5432
Database: mydb
Username: postgres
Password: (20+ char password)
```

Connection string: `postgresql+psycopg2://postgres:password@myapp.c5xyzabc.us-east-1.rds.amazonaws.com:5432/mydb`

### Example 3: Local SQLite (Testing)

```
Database File Path: /Users/alice/data/sample.db
```

Connection string: `sqlite:////Users/alice/data/sample.db`

### Example 4: Cloud MySQL (Google Cloud)

```
Host: 34.56.78.90 (public IP)
Port: 3306
Database: analytics
Username: cloud_user
Password: (cloud password)
```

Connection string: `mysql+pymysql://cloud_user:password@34.56.78.90:3306/analytics`

---

## Part 6: Common Pitfalls & Solutions

| Problem | Cause | Solution |
|---------|-------|----------|
| "Connection refused" | DB not running | Start database service |
| "Access denied" | Wrong password | Verify in native client |
| "No tables found" | Wrong database | `USE mydb;` then create table |
| "Timeout" | Long query | Reduce row limit or use WHERE clause |
| "database is locked" | SQLite open elsewhere | Close other apps accessing file |
| "host not found" | Typo in hostname | Check spelling, use IP instead |
| "port already in use" | Another process using port | Kill process or change port |
| "Too many connections" | Connection pool exhausted | Restart app or database |

---

## Part 7: Performance Tips

**Fast connections:**
- Use local database when possible (no network latency)
- Index columns used in WHERE clauses
- Set row limit to match your screen (10,000-50,000)
- Close app when done (releases connection)

**Slow connections:**
- Network latency (cloud databases 100-500ms)
- Large tables without indexes
- Complex queries with joins
- Firewall rules checking

**Optimize your queries:**
```sql
-- FAST: Filter at database level
SELECT * FROM orders WHERE status='pending' LIMIT 10000

-- SLOW: Load everything then filter in Python
SELECT * FROM orders LIMIT 10000000
```

---

## Part 8: Getting Help

If connection still fails:

1. **Test in native client first:**
   - `psql` for PostgreSQL
   - `mysql` for MySQL
   - `sqlite3` for SQLite
   - `sqlcmd` for SQL Server

2. **Check database is accepting connections:**
   ```bash
   # PostgreSQL
   pg_isready -h localhost -p 5432
   
   # MySQL
   mysql -h localhost -e "SELECT 1"
   
   # SQLite
   sqlite3 /path/to/db.db "SELECT 1"
   ```

3. **Verify firewall allows port:**
   ```bash
   # Linux/macOS
   lsof -i :5432  # Port 5432
   telnet localhost 5432  # Try to connect
   ```

4. **Check database logs:**
   - PostgreSQL: `/var/log/postgresql/`
   - MySQL: `/var/log/mysql/`
   - SQL Server: Event Viewer (Windows)

---

## Summary: Is It Reliable?

✅ **Yes, for:**
- Development & testing
- Small-to-medium databases (<10M rows)
- Read-only operations
- Persistent data sources

⚠️ **With caution for:**
- Production (needs monitoring)
- Real-time data (load once per session)
- Very large tables (limit to 100k rows)
- High-concurrency (one user at a time)

❌ **Not for:**
- Writing data (read-only)
- Real-time streaming
- 1B+ row tables
- Complex transactions
