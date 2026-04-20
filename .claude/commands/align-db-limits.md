Check that the DB row-limit ceiling is consistent across every layer, and fix any mismatches.

## Target value
`1_000_000` (1 million rows). This is the canonical limit for all DB ingestion paths.

## Files to check (in this order)

| File | What to grep | Expected value |
|------|-------------|----------------|
| `src/app.py` | `st.slider('Row Limit', ...` default arg (3rd positional) | `1_000_000` |
| `src/utils/db_ingestion.py` | any `LIMIT` constant or `nrows` default | `1_000_000` (if present) |
| `src/core/connectors/postgres.py` | `self.config.get("row_limit", ...)` | `1_000_000` |
| `src/core/connectors/mysql.py` | `config.get('row_limit', ...)` | `1_000_000` |
| `src/core/connectors/*.py` (any future connectors) | same `row_limit` pattern | `1_000_000` |

## Steps

1. Grep each file for the row-limit value using the patterns above.
2. Print a one-line status for each: `✓ aligned` or `✗ found <value>`.
3. For every misaligned file, apply the fix — update the default to `1_000_000`.
4. Re-grep to confirm all values are now `1_000_000`.
5. Also check that the slider **min** (`1_000`) and **max** (`2_000_000`) in `app.py` are not accidentally set lower than 1M.
6. Report a final summary: how many files were checked, how many were fixed, and confirm the canonical limit is `1_000_000` everywhere.

## Do not change
- The slider **max** (currently `2_000_000`) — users can still load up to 2M rows if they move the slider.
- Any hard-coded `LIMIT` inside test files — those use smaller numbers intentionally.
- The `step=10_000` slider increment.
