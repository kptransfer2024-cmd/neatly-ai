Audit a detector module for quality, complexity, and performance. The argument is a detector name (e.g. `outlier_detector`) or blank to audit all detectors.

## What to report (in this exact order, one section per detector)

### 1. Quality checklist
Review the file and report pass/fail on each:
- [ ] All thresholds are named module-level constants (no magic numbers inline)
- [ ] Vectorized pandas/numpy operations — no per-row Python loops
- [ ] Guards for: empty DataFrame, no matching columns, all-NaN columns, constant/degenerate columns
- [ ] Return shape follows project convention: `list[dict]`, each dict has a `'type'` key (except `missing_value_detector.detect_missing` which predates this)
- [ ] No raw data rows sent anywhere outside the module (project hard rule)
- [ ] Docstring explains the method and returned dict shape

### 2. Complexity
State time and space complexity in Big-O, and name the dominant operation. Flag any op that's worse than O(n·m) where n=rows, m=columns.

### 3. Test coverage
Run `pytest tests/test_<name>.py -v`. Report:
- Pass/fail count
- Whether edge cases are covered: empty df, no matching cols, constant cols, NaN ignored, both-tails (if applicable), sample-index cap, threshold boundary
- Any missing edge case → propose a new test

### 4. Benchmark
Run this on realistic synthetic data and report `ms/call`:
```python
import pandas as pd, numpy as np, time
np.random.seed(42)
df = pd.DataFrame(np.random.randn(100000, 20), columns=[f'c{i}' for i in range(20)])
# For string-focused detectors, adapt df accordingly (see existing benchmark in /run-tests transcripts).
from detectors.<name> import detect  # or detect_missing
t0 = time.perf_counter()
for _ in range(5): detect(df)
print(f'<name>: {(time.perf_counter()-t0)/5*1000:.0f} ms/call')
```
Flag anything over 500 ms/call on this size.

### 5. Verdict
One of: **LGTM**, **MINOR (list them)**, **BLOCKER (describe)**.
If BLOCKER or MINOR items exist, propose concrete fixes — do NOT apply them automatically; wait for user approval.

## Scope rules
- Only audit. Do not edit files unless the user explicitly approves a proposed fix.
- If argument is blank, audit every file in `detectors/` that is fully implemented (not a `raise NotImplementedError` stub).
- Keep the total report under 40 lines per detector.
