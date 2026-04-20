# /suggest-impl

Analyze a file's stability gaps and generate ranked, ready-to-apply implementation suggestions.

**Argument:** file path or module name (e.g. `utils/diff_engine.py` or `code_snippets`). Required.

---

## Workflow

### 1. Gather context

Run these in parallel:

- **Read** the target file in full
- **Read** its test file (if it exists)
- **Run** `/stability-check <file>` to get the stability score and known issues
- **Grep** for all callers of the file's public functions across the codebase

### 2. Classify each gap by severity

| Severity | Criteria |
|----------|----------|
| 🔴 BLOCKER | Bug in production logic (wrong key lookup, SQL injection, schema violation causing crashes) |
| 🟠 HIGH | No test coverage on important logic, fragile patterns that will break |
| 🟡 MED | Missing type hints, undocumented public API, DRY violation that causes drift risk |
| 🟢 LOW | Minor style issues, dead code, naming inconsistencies |

### 3. For each gap, produce a concrete suggestion

Each suggestion must include:
- **Title** — one sentence naming the problem
- **Why it matters** — consequence if left unfixed
- **Current code** — the exact snippet that is broken/missing
- **Suggested code** — the exact replacement/addition
- **Effort** — S (< 15 min) / M (15–60 min) / L (> 1 hour)

### 4. Present suggestions in priority order

Format:

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
IMPLEMENTATION SUGGESTIONS: utils/diff_engine.py
Stability: 🟠 FRAGILE (38/100)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

[1] 🟠 HIGH — Add test coverage for compute_diff()
    Why: compute_diff() is called on every action in the UI and handles NaN
         equality — zero coverage means silent regressions if logic changes.
    Effort: M

    Suggested tests to add in tests/test_diff_engine.py:
    • test_compute_diff_changed_values — modify a cell, verify it appears in diff
    • test_compute_diff_added_column — add a column, verify it appears
    • test_compute_diff_dropped_rows — drop rows, verify row_count_delta
    • test_compute_diff_nan_equality — NaN in both before/after should NOT count as changed
    • test_compute_diff_empty_dataframes — both empty → diff with zeros

[2] 🟡 MED — Fix render_diff() missing type hint and docstring
    Why: render_diff() is a public function called from app.py but has no
         signature documentation — callers can't know the expected dict shape.
    Effort: S

    Current:
        def render_diff(diff):

    Suggested:
        def render_diff(diff: dict) -> None:
            """Render a compute_diff() result as Streamlit components."""

[3] 🟢 LOW — Replace _get_ext string splitting with pathlib
    Why: pathlib.Path.suffix is standard and handles edge cases like
         dotfiles ('', '.gitignore') correctly.
    Effort: S

    Current:
        return '.' + filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''

    Suggested:
        from pathlib import Path
        return Path(filename).suffix.lower()
```

### 5. Ask before implementing

After printing all suggestions, ask:

```
Found N suggestions (X blockers, Y high, Z med, W low).
Which would you like me to implement?
  [A] All of them
  [1] Suggestion 1 only
  [1,3] Suggestions 1 and 3
  [skip] None — just noting for now
```

Wait for the user's choice, then implement only the selected suggestions.

### 6. After implementing

- Run `pytest tests/` to confirm nothing broke
- Run `/stability-check <file>` again to show the new score
- Commit with `fix:` or `feat:` prefix via `/push` if the score improved
