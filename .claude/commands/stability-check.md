# /stability-check

Analyze a Python file for stability signals and produce a scored report.

**Argument:** file path or module name (e.g. `utils/diff_engine.py` or `missing_value_detector`). Omit to scan all project files.

---

## Workflow

### 1. Resolve the target

If an argument was provided, locate the file:
- Try exact path first
- Try `detectors/<arg>.py`, `utils/<arg>.py`, `src/<arg>.py`
- If no argument, glob `**/*.py` excluding `__pycache__`, `tests/`, `.claude/`

### 2. For each target file, collect signals

Run these checks **in parallel** using the Read and Grep tools:

**A. Test coverage**
- Does a corresponding `tests/test_<filename>.py` exist?
- If yes, count the number of `def test_` functions in it
- Score: 0 = no test file, 1 = file exists but <5 tests, 2 = 5–15 tests, 3 = 15+ tests

**B. Error handling**
- Count `try/except` blocks in the file
- Check if public functions have `raise` with meaningful messages
- Score: 0 = none, 1 = some, 2 = thorough

**C. Type hints**
- Count function definitions (`def `) vs those with `: ` annotations on params
- Check for `-> ` return type annotations
- Score: 0 = none, 1 = partial, 2 = full

**D. Docstrings**
- Module-level docstring present?
- Public function docstrings present?
- Score: 0 = none, 1 = module only, 2 = module + public functions

**E. Schema compliance** (detectors only)
- Does the detector return `'columns'` (list) or `'column'` (string)?
- Does it include `detector`, `severity`, `row_indices`, `summary`, `actions`?
- Score: 0 = non-compliant, 1 = partial, 2 = fully compliant

**F. DRY / duplication risks**
- Grep for any dict/constant that appears to be copied from another file
- Known risks: `pattern_regexes`, `_DOMAIN_BOUNDS`, `_PARSERS`
- Score: 0 = known duplication, 1 = clean

### 3. Compute stability score

```
stability_score = (test_score * 3) + (error_score * 2) + type_score + doc_score + [schema_score if detector] + dry_score
max_score = 11 (or 13 for detectors)
pct = stability_score / max_score * 100
```

**Stability label:**
- 85–100% → 🟢 STABLE
- 65–84%  → 🟡 NEEDS ATTENTION
- 40–64%  → 🟠 FRAGILE
- 0–39%   → 🔴 CRITICAL

### 4. Output the report

For each file, print:

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📁 <filepath>  [<LABEL> — <score>/<max>]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Tests        : [●●●○] 2/3  (12 test functions)
Error handling: [●●○○] 1/2  (2 try/except blocks)
Type hints   : [●●○○] 1/2  (partial)
Docstrings   : [●○○○] 1/2  (module only)
Schema       : [●●○○] N/A  (not a detector)
DRY          : [●○○○] 0/1  ⚠ pattern_regexes duplicated

Known issues:
  - [HIGH] No test file — diff logic is untested
  - [MED]  render_diff() has no type hints or docstring
  - [LOW]  _build_before_after_frame could use pathlib.Path for ext handling
```

If scanning all files, print a summary leaderboard at the end:

```
STABILITY LEADERBOARD
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🟢 STABLE          orchestrator.py, transformation_executor.py, ...
🟡 NEEDS ATTENTION app.py, context_interpreter.py, ...
🟠 FRAGILE         utils/diff_engine.py, utils/code_snippets.py, ...
🔴 CRITICAL        (none)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Run /suggest-impl <file> for actionable fixes on any fragile file.
```

### 5. Don't auto-fix

This command is read-only. It reports findings only. To implement fixes, use `/suggest-impl`.
