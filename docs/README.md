# Documentation Index

Comprehensive guides for using and developing Neatly AI.

**Start with:** [../README.md](../README.md) (main project overview)

## Getting Started

1. **[DATABASE_INPUT_SETUP.md](DATABASE_INPUT_SETUP.md)** — How to use the database input feature
   - User workflow (upload from PostgreSQL, MySQL, SQLite, SQL Server)
   - Dependency installation
   - Code structure overview
   - MCP server configuration for Claude Code development
   - Testing instructions
   - Security notes
   - Troubleshooting

2. **[DATABASE_CONNECTION_GUIDE.md](DATABASE_CONNECTION_GUIDE.md)** — Reliability & how-to
   - Detailed reliability assessment (what's reliable, what requires care)
   - Step-by-step connection guides for each database type
   - Troubleshooting flowchart
   - Production reliability checklist
   - Real connection examples
   - Performance optimization tips

## Implementation Details

3. **[IMPLEMENTATION_COMPLETE.md](IMPLEMENTATION_COMPLETE.md)** — What was built
   - Layer 1: User-facing database UI in Streamlit
   - Layer 2: MCP server configuration
   - Layer 3: Database ingestion module
   - Files changed and test coverage
   - Database support matrix
   - Usage examples
   - Deployment checklist

## Quality Assurance

4. **[QA_REPORT.md](QA_REPORT.md)** — Test structure and quality findings
   - 70+ comprehensive tests for schema compliance
   - Critical issues found and documented
   - Test patterns and best practices
   - Coverage analysis

5. **[TESTING_SUMMARY.md](TESTING_SUMMARY.md)** — Test results and findings
   - Test execution summary
   - Integration test results
   - Discovered issues during testing
   - Recommendations

## Quick Links

- **Main README** → [../README.md](../README.md)
- **Project Instructions** → [../CLAUDE.md](../CLAUDE.md)
- **Source Code** → [../src/](../src/)
- **Tests** → [../src/tests/](../src/tests/)

## Feature by Feature

### Database Input

All guides related to connecting external databases:
- [DATABASE_INPUT_SETUP.md](DATABASE_INPUT_SETUP.md) — Setup & configuration
- [DATABASE_CONNECTION_GUIDE.md](DATABASE_CONNECTION_GUIDE.md) — Connection details
- [IMPLEMENTATION_COMPLETE.md](IMPLEMENTATION_COMPLETE.md) — Technical implementation

### Data Quality Detectors

The app includes 12+ detectors for different data quality issues. See source code in `src/detectors/`.

### File Upload

Original file-based input. See `src/utils/file_ingestion.py`.

## For Developers

If working with Claude Code:
1. Read [DATABASE_INPUT_SETUP.md](DATABASE_INPUT_SETUP.md) "Part 4: MCP Server Integration"
2. Configure `.claude/settings.local.json` with database connection
3. Use MCP tools to query database directly

If adding new detectors:
1. Create `src/detectors/your_detector.py`
2. Implement `detect(df) -> list[dict]` following issue dict schema
3. Add tests in `src/tests/test_your_detector.py`
4. Run: `pytest src/tests -v`

## File Manifest

| File | Purpose |
|------|---------|
| DATABASE_INPUT_SETUP.md | User & dev guide for database input |
| DATABASE_CONNECTION_GUIDE.md | Reliability assessment + step-by-step guides |
| IMPLEMENTATION_COMPLETE.md | Technical summary of what was built |
| QA_REPORT.md | Comprehensive test report |
| TESTING_SUMMARY.md | Test results & findings |
