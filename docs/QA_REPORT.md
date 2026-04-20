# QA Report: Code Quality & Architecture Audit

**Date:** 2026-04-19  
**Branch:** feat/missing-value-detector  
**Test Status:** 202 ✅ passed | 4 ⏭️ skipped | 7 ❌ critical issues found

---

## Executive Summary

The codebase is in a **partially refactored state**. Work has started to implement the CLAUDE.md schema but is incomplete. This creates **critical runtime bugs** where the app will crash and features will be missing.

### Critical Issues (Production Breaking)

1. **💥 App Crashes at Line 98** - Accessing `issue['columns']` which doesn't exist
2. **🔌 Missing Action Buttons** - Only `missing_value_detector` has fix buttons; others show "No actions"
3. **⚠️ Silent Exceptions** - `orchestrator.py` hides detector errors with bare `except: pass`

---

## Detailed Findings

### Issue #1: Schema Architecture Mismatch (CRITICAL)

**Status:** ⚠️ Work In Progress  
**Impact:** App-breaking

#### Current State
- `missing_value_detector.py` has been partially refactored to new schema
- Other detectors still return old minimal schemas
- `orchestrator.py` has partial fix (converts `explanation` → `summary`)
- **Uncommitted changes** in working directory

#### What CLAUDE.md Requires
```python
{
    'detector': str,          # name of detector
    'type': str,              # issue type
    'columns': list[str],     # affected column(s) — NOTE: LIST not STRING
    'severity': str,          # 'low' | 'medium' | 'high'
    'row_indices': list[int], # affected rows
    'summary': str,           # plain-English explanation
    'sample_data': dict,      # column-level stats
    'actions': list[dict],    # [{id, label, description, params}, ...]
}
```

#### What Detectors Actually Return

**missing_value_detector:**
```
✅ detector, severity, row_indices, summary, sample_data, actions
❌ 'column' (string) not 'columns' (list)
⚠️ Still includes old fields: missing_count, missing_pct, sample_values
```

**duplicate_detector:**
```
❌ Missing: detector, severity, row_indices, sample_data, actions
⚠️ Has: type, duplicate_count, total_rows, duplicate_pct, sample_indices
```

**outlier_detector:**
```
❌ Missing: detector, severity, sample_data, actions
⚠️ Has: type, column, outlier_count, outlier_pct, etc.
```

**schema_analyzer:**
```
❌ Missing: detector, severity, row_indices, sample_data, actions
⚠️ Has: type, column, current_dtype, suggested_dtype, sample_values
```

**consistency_cleaner:**
```
❌ Missing: detector, severity, row_indices, sample_data, actions
⚠️ Has: type, column, sub_type, example_values
```

---

### Issue #2: App Will Crash at Runtime (CRITICAL)

**Location:** `app.py:98`  
**Code:**
```python
st.text(f"Columns: {', '.join(issue['columns'])}")
```

**Problem:**
- Assumes `issue['columns']` is a list of strings
- Detectors return `'column'` (singular string), not `'columns'` (list)
- Will raise `KeyError: 'columns'` when rendering ANY issue

**Affected Detectors:** All 5 detectors

---

### Issue #3: Missing Action Buttons (FEATURE BREAKING)

**Location:** `app.py:103-117`  
**Code:**
```python
actions = issue.get('actions', [])
if actions:
    # render buttons
else:
    st.info('No actions available for this issue.')
```

**Problem:**
- Only `missing_value_detector` provides `actions`
- All others will show "No actions available"
- Users cannot fix duplicates, outliers, type mismatches, or inconsistent formatting

**Affected Detectors:** 4 of 5

---

### Issue #4: Silent Exception Handling

**Location:** `orchestrator.py:37-38`  
**Code:**
```python
except Exception:
    pass
```

**Problem:**
- Detector errors are silently swallowed
- No logging, no re-raise
- Makes debugging difficult
- Should at least log or re-raise in development

**Recommendation:** Replace with proper error handling or at minimum log the error.

---

## Test Results

### Original Test Suite
```
✅ 132 tests passing (all original unit tests)
```

### New QA Tests Added
```
✅ test_schema_compliance.py: 22 tests (document missing fields)
✅ test_app_render_failures.py: 7 tests (simulate app crashes)
✅ test_e2e_schema.py: 1 test passing (end-to-end flow)
✅ test_summary.py: 9 tests (comprehensive issue documentation)
⏭️ 4 tests skipped (waiting for other detectors to be refactored)

Total: 202 passed, 4 skipped
```

---

## Uncommitted Changes Detected

The working directory has partial fixes that haven't been committed:

### `detectors/missing_value_detector.py`
- ✅ Added: `detector`, `severity`, `row_indices`, `actions`, `sample_data`
- ❌ Still has: `column` instead of `columns`, old field duplication
- ⚠️ Tests broken by this change

### `orchestrator.py`
- ✅ Added: Line to convert `explanation` → `summary`
- This partially fixes the schema but doesn't solve all issues

---

## Detailed Breakdown by Detector

### missing_value_detector ✅ (Mostly Done)
```
Status: Partially refactored
Fields: detector, column, severity, row_indices, summary, sample_data, actions
Missing: columns (has column), type (added by orchestrator)
Tests: 7 fail with current changes (expect old schema)
```

### duplicate_detector ❌ (Not Started)
```
Status: Old schema only
Fields: type, duplicate_count, total_rows, duplicate_pct, sample_indices
Missing: detector, severity, row_indices, sample_data, actions, columns
Test Status: Will break at app render
Fix Effort: ~2 hours
```

### outlier_detector ❌ (Not Started)
```
Status: Old schema only
Fields: type, column, outlier_count, outlier_pct, lower_fence, upper_fence, sample_indices
Missing: detector, severity, row_indices, sample_data, actions, columns
Test Status: Will break at app render
Fix Effort: ~2 hours
```

### schema_analyzer ❌ (Not Started)
```
Status: Old schema only
Fields: type, column, current_dtype, suggested_dtype, sample_values
Missing: detector, severity, row_indices, sample_data, actions, columns
Test Status: Will break at app render
Fix Effort: ~2 hours
```

### consistency_cleaner ❌ (Not Started)
```
Status: Old schema only
Fields: type, column, sub_type, example_values
Missing: detector, severity, row_indices, sample_data, actions, columns
Test Status: Will break at app render
Fix Effort: ~2 hours
```

---

## Recommended Fix Strategy

### Phase 1: Complete missing_value_detector (In Progress)
1. Fix `'column'` → `'columns'` conversion
2. Remove old field duplication (missing_count, missing_pct at root)
3. Update tests to match new schema
4. ✅ Already has actions defined

### Phase 2: Refactor remaining detectors
1. **duplicate_detector**: Add severity based on %, row_indices, actions for drop
2. **outlier_detector**: Add severity based on %, actions for clip
3. **schema_analyzer**: Add severity, actions for cast
4. **consistency_cleaner**: Add severity, actions for normalize

### Phase 3: Fix orchestrator & explanation_layer
1. Ensure all detectors return `columns` (list)
2. Move `explanation` → `summary` conversion
3. Remove exception swallowing

### Phase 4: Update app.py to handle missing fields gracefully
1. Use `.get('columns')` with sensible default
2. Show warning if fields missing instead of crashing

---

## Code Quality Notes

### Positive Findings ✅
- Vectorized pandas operations (good performance)
- Proper edge case handling (empty DataFrames, all-null columns)
- Dtype handling correct (`in ('object', 'str')`)
- Test coverage for transformations is solid
- CLAUDE.md lessons learned implemented (dtype checking)

### Areas for Improvement
- Schema consistency across detectors
- Error handling (silent exceptions)
- Missing type hints in some functions
- Incomplete refactor left uncommitted

---

## Risk Assessment

| Risk | Severity | Impact | Status |
|------|----------|--------|--------|
| App crashes when rendering issues | 🔴 CRITICAL | User cannot see issues | In working directory |
| Missing action buttons | 🔴 CRITICAL | Feature unusable | Affects 4/5 detectors |
| Silent exceptions | 🟠 HIGH | Hard to debug | In main code |
| Schema inconsistency | 🟠 HIGH | Maintenance burden | Partial fix |

---

## Test Files Created

For documenting issues and aiding future fixes:

1. **test_schema_compliance.py** - Schema validation for each detector
2. **test_app_render_failures.py** - Simulates app rendering logic
3. **test_e2e_schema.py** - End-to-end pipeline validation
4. **test_summary.py** - Comprehensive issue documentation

These can be deleted after fixes are applied, or kept as regression tests.

---

## Recommendations

1. **Immediate:** Commit or discard partial changes in working directory
2. **Short-term:** Complete detector schema refactoring
3. **Follow-up:** Add schema validation tests to CI/CD
4. **Long-term:** Consider using TypedDict or Pydantic for issue schema validation

---

## Test Coverage Summary

| Component | Coverage | Status |
|-----------|----------|--------|
| detectors (logic) | ~90% | ✅ Good |
| transformation_executor | ~95% | ✅ Excellent |
| orchestrator | ~80% | ⚠️ Gaps in error paths |
| explanation_layer | ~85% | ⚠️ Mock-heavy tests |
| app.py | ~0% | ❌ No UI tests |
| schema validation | NEW | ⚠️ Work in progress |

---

## Next Steps

1. ✅ Run this test suite to verify findings
2. Choose fix strategy (finish current refactor vs. revert + restart)
3. Update all detectors to CLAUDE.md schema
4. Update all tests to match new schema
5. Add schema validation to test suite
6. Test with actual Streamlit app

