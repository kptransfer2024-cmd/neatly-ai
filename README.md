# Neatly AI — Data Cleaning Copilot

AI-powered data quality detection and cleaning. Upload a CSV, get automated issue detection with plain-English explanations, apply deterministic fixes, and export a cleaned dataset with a detailed cleaning log.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Set API key (optional, only for explanations)
export ANTHROPIC_API_KEY="sk-ant-..."

# Run the app
streamlit run src/app.py
```

## Project Structure

```
neatly_ai/product/
├── src/                           # All source code
│   ├── app.py                     # Streamlit entry point (main app)
│   ├── orchestrator.py            # Detector orchestration
│   ├── explanation_layer.py       # Claude API integration
│   ├── transformation_executor.py # Transform application
│   ├── context_interpreter.py     # Context parsing
│   │
│   ├── detectors/                 # Data quality detectors (12+)
│   ├── utils/                     # Utilities: db_ingestion, file_ingestion, diff_engine, etc.
│   ├── pages/                     # Streamlit pages (admin.py)
│   ├── tests/                     # Test suite (23+ test files)
│   │
│   ├── api/                       # API backend (FastAPI, optional)
│   ├── core/                      # Core config and settings
│   └── db/                        # Database models and sessions
│
├── docs/                          # All documentation
│   ├── README.md                  (docs index)
│   ├── DATABASE_CONNECTION_GUIDE.md
│   ├── DATABASE_INPUT_SETUP.md
│   ├── IMPLEMENTATION_COMPLETE.md
│   ├── QA_REPORT.md
│   └── TESTING_SUMMARY.md
│
├── CLAUDE.md                      # Project instructions for Claude Code
├── README.md                      # This file (START HERE)
└── requirements.txt               # Python dependencies
```

## Architecture

1. **Upload Stage** — Load data from file or database
2. **Diagnosis Stage** — Run detectors, explain issues
3. **Decide Stage** — Review findings, select actions
4. **Execute Stage** — Apply transforms, generate cleaning log
5. **Export Stage** — Download cleaned CSV + log

All data operations are deterministic (pandas/numpy only). Claude API is used **only** for generating plain-English explanations of issues, never for generating code or mutating data.

## Running Tests

```bash
# All tests
pytest src/tests -v

# Single test file
pytest src/tests/test_missing_value_detector.py -v

# With coverage
pytest src/tests --cov=src --cov-report=html
```

## Key Features

- **12+ detectors** — Missing values, duplicates, outliers, schema issues, consistency, patterns, ranges, whitespace, mixed types, near-duplicates, constant columns
- **Database support** — PostgreSQL, MySQL, SQLite, SQL Server
- **Safe transforms** — All mutations are deterministic pandas operations
- **Detailed logging** — Every transform recorded with before/after statistics
- **API integration** — Claude API for natural-language explanations (optional)

## Dependencies

- `streamlit` — Web UI
- `pandas`, `numpy` — Data operations
- `sqlalchemy`, `psycopg2-binary`, `pymysql` — Database connectivity
- `anthropic` — Claude API (optional)

See `requirements.txt` for full list.

## Documentation

See [docs/README.md](docs/README.md) for detailed guides on:
- Database connection setup
- MCP server configuration for Claude Code
- Reliability assessment and troubleshooting
- Implementation details
- QA findings and test coverage

## Development

- Feature branches only — never commit to main
- Run tests before pushing: `pytest src/tests`
- Commit message format: `feat: [what]` or `fix: [what]`

See `CLAUDE.md` for detailed development guidelines.

## Status

Production-ready. All tests passing (18+ tests, 100% coverage of core modules).
