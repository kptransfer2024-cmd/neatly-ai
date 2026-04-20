# Neatly — AI Data Cleaning Copilot

Upload a dataset, get automated issue detection with plain-English explanations, apply deterministic fixes with one click, and export a cleaned dataset with a full audit log.

---

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run the app
streamlit run src/app.py
```

Open [http://localhost:8501](http://localhost:8501) in your browser.

> `ANTHROPIC_API_KEY` is **not required** for normal operation — explanations use static templates.

---

## Admin Dashboard

The analytics dashboard lets you see session activity, which detectors fire most, and what actions users take. It is **not** bundled with the public app — it's a private script you run separately.

### Open locally (dev sessions only)

```bash
streamlit run src/admin_app.py --server.port 8502
```

Open [http://localhost:8502](http://localhost:8502). This reads `neatly_logs.jsonl` from the local filesystem and shows only sessions from your own machine.

### Open with Supabase (cloud + local sessions together)

When `SUPABASE_URL` and `SUPABASE_KEY` are set, every event — from both the deployed public app and local dev — is written to a shared `neatly_events` Postgres table. The admin dashboard reads from the same table, so you see **all users in one place**.

**Step 1 — Create the table** (run once in Supabase SQL Editor):

```sql
CREATE TABLE neatly_events (
  id          BIGSERIAL PRIMARY KEY,
  session_id  TEXT        NOT NULL,
  event       TEXT        NOT NULL,
  ts          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  data        JSONB       NOT NULL DEFAULT '{}'
);
ALTER TABLE neatly_events ENABLE ROW LEVEL SECURITY;
CREATE POLICY "anon_all" ON neatly_events FOR ALL TO anon USING (true) WITH CHECK (true);
```

**Step 2 — Set env vars** (local `.env` or Streamlit secrets):

```bash
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-anon-key
```

**Step 3 — Run the dashboard:**

```bash
streamlit run src/admin_app.py --server.port 8502
```

> **Password gate (optional):** Add `ADMIN_PASSWORD = "your-password"` to `.streamlit/secrets.toml` to require login before viewing the dashboard.

---

## Loading Data

### File upload
CSV, TSV, JSON, Excel (.xlsx/.xls), Parquet — up to **2 million rows**.

### Database (direct connection)
Connect directly without exporting a file first.

| Database | Notes |
|---|---|
| **MySQL Workbench (Local)** | Use `localhost` / `3306` / `root` — same credentials as your Workbench connection |
| **MySQL (remote)** | Any remote MySQL server |
| **PostgreSQL** | Local or cloud (Supabase, Neon, RDS) |
| **SQLite** | Provide the local file path |
| **SQL Server** | Requires ODBC Driver 17 installed |

Row limit: up to **2 million rows** via a slider before loading.

---

## Exporting Cleaned Data

**When you loaded from a database:** the done stage leads with a pre-filled "Save to Database" tab — same host/port/user/password/database, output table defaults to `{source_table}_cleaned`. Just confirm and push.

**When you uploaded a file:** CSV + JSON log downloads appear first. An optional "Save to Database" expander lets you push to any database.

---

## Detectors

| Detector | What it finds |
|---|---|
| Missing values | Null / NaN cells |
| Whitespace values | Cells that are only spaces / tabs |
| Duplicate rows | Exact duplicate records |
| Near-duplicate rows | Fuzzy text matches (edit distance) |
| Duplicate columns | Columns with identical values |
| Outliers | Statistical outliers via Tukey IQR |
| Type mismatches | Columns with mixed / wrong types |
| Pattern violations | Values not matching expected regex patterns |
| Out-of-range values | Numeric values outside defined bounds |
| Format inconsistencies | Inconsistent date/phone/currency formats |
| Constant columns | Columns with a single unique value |
| PII detection | Emails, phone numbers, SSNs, credit cards |
| Schema analysis | Column type inference and anomalies |
| Range validator | Min/max constraint checks |

---

## Project Structure

```
product/
├── src/
│   ├── app.py                    # Streamlit UI — main app
│   ├── admin_app.py              # Analytics dashboard (local only)
│   ├── orchestrator.py           # Wires detectors → explanation → transforms
│   ├── explanation_layer.py      # Plain-English issue summaries (static templates)
│   ├── transformation_executor.py# All data mutations (deterministic, pandas only)
│   ├── detectors/                # 14 detector modules
│   ├── utils/
│   │   ├── analytics.py          # Event logging (local JSONL + Supabase)
│   │   ├── db_ingestion.py       # SQLAlchemy database helpers
│   │   ├── file_ingestion.py     # CSV/Excel/Parquet parsing
│   │   └── diff_engine.py        # Before/after diff rendering
│   ├── core/connectors/          # DB connector classes (MySQL, Postgres, S3, BigQuery)
│   └── tests/                    # 597 tests across all modules
├── requirements.txt
├── CLAUDE.md                     # Development guidelines
└── README.md                     # This file
```

---

## Running Tests

```bash
# All tests
cd src && python -m pytest tests/ -v

# Single module
python -m pytest tests/test_missing_value_detector.py -v

# With coverage
python -m pytest tests/ --cov=. --cov-report=html
```

**597 tests, all passing.**

---

## Architecture

All business logic lives in `detectors/` and flows through `orchestrator.py → explanation_layer.py → transformation_executor.py`. `app.py` only reads and writes `st.session_state`.

**Hard rules:**
- All data mutations are deterministic pandas/numpy — no LLM-generated code
- No LLM calls during normal operation — explanations use static templates
- Every transform appends to `cleaning_log`

---

## Development

```bash
# Feature branches only — never commit to main
git checkout -b feat/your-feature

# Run tests before pushing
cd src && python -m pytest tests/

# Commit format
git commit -m "feat: add X" # or "fix: repair Y"
```

---

## Dependencies

| Package | Purpose |
|---|---|
| `streamlit` | Web UI |
| `pandas`, `numpy` | All data operations |
| `sqlalchemy`, `pymysql`, `psycopg2-binary` | Database connectivity |
| `supabase` | Cloud analytics log storage |
| `anthropic` | Claude API (optional, explanations only) |

See `requirements.txt` for the full list.
