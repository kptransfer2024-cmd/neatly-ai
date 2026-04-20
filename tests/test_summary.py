"""
QA SUMMARY TEST - Comprehensive schema and runtime issues found

UNCOMMITTED CHANGES DETECTED:
- detectors/missing_value_detector.py: Partial refactor to new schema
- orchestrator.py: Added 'explanation' → 'summary' conversion
- .claude/settings.local.json: Configuration changes

TEST RESULTS SUMMARY:
✅ 132 original unit tests PASS
❌ Missing value detector: Tests fail (expects old schema)
⚠️  4 other detectors: NOT refactored, will break at app render time
⚠️  App.py: Will crash at line 98 when 'columns' field is missing
"""

import pandas as pd
import pytest

from detectors.missing_value_detector import detect_missing
from detectors.duplicate_detector import detect as detect_duplicates
from detectors.outlier_detector import detect as detect_outliers


class TestCriticalRenderingIssues:
    """These are REAL BUGS that will occur at runtime when app tries to render."""

    def test_app_crashes_on_duplicate_issue_line_98(self):
        """CRASH: app.py line 98 tries to join issue['columns']

        But duplicate_detector returns no 'columns' field.
        This WILL crash the app when rendering a duplicate issue.
        """
        df = pd.DataFrame({'a': [1, 1, 2], 'b': [1, 1, 3]})
        issues = detect_duplicates(df)

        if issues:
            issue = issues[0]
            # This is what app.py tries to do:
            with pytest.raises(KeyError, match='columns'):
                ', '.join(issue['columns'])

    def test_app_crashes_on_missing_value_line_98(self):
        """CRASH: app.py line 98 tries to join issue['columns']

        But missing_value_detector has 'column' (singular), not 'columns' (list).
        This WILL crash the app even though missing_value has the refactor started.
        """
        df = pd.DataFrame({'age': [25, None, 35]})
        issues = detect_missing(df)

        if issues:
            issue = issues[0]
            # missing_value_detector provides 'column' not 'columns'
            assert 'column' in issue
            assert 'columns' not in issue
            # This is what app.py tries to do:
            with pytest.raises(KeyError, match='columns'):
                ', '.join(issue['columns'])

    def test_duplicate_detector_no_fix_buttons(self):
        """MISSING FEATURE: duplicate_detector has no 'actions' field

        User will see "No actions available for this issue" and cannot fix duplicates.
        """
        df = pd.DataFrame({'a': [1, 1, 2], 'b': [1, 1, 3]})
        issues = detect_duplicates(df)

        if issues:
            issue = issues[0]
            actions = issue.get('actions', [])
            assert len(actions) == 0, "No action buttons for duplicates"

    def test_outlier_detector_no_fix_buttons(self):
        """MISSING FEATURE: outlier_detector has no 'actions' field

        User will see "No actions available" and cannot fix outliers.
        """
        df = pd.DataFrame({'values': [1, 2, 3, 4, 100]})
        issues = detect_outliers(df)

        if issues:
            issue = issues[0]
            actions = issue.get('actions', [])
            assert len(actions) == 0, "No action buttons for outliers"


class TestSchemaInconsistency:
    """These show the inconsistent state of detectors."""

    def test_only_missing_value_has_actions(self):
        """Only missing_value_detector provides actions for UI buttons."""
        df_missing = pd.DataFrame({'x': [1, None, 3]})
        issues_missing = detect_missing(df_missing)
        assert len(issues_missing[0]['actions']) > 0

        df_dup = pd.DataFrame({'a': [1, 1, 2]})
        issues_dup = detect_duplicates(df_dup)
        if issues_dup:
            assert 'actions' not in issues_dup[0]

        df_outlier = pd.DataFrame({'v': [1, 2, 3, 100]})
        issues_outlier = detect_outliers(df_outlier)
        if issues_outlier:
            assert 'actions' not in issues_outlier[0]

    def test_detectors_use_different_key_styles(self):
        """Detectors return inconsistent key names."""
        df_missing = pd.DataFrame({'x': [1, None, 3]})
        issue_missing = detect_missing(df_missing)[0]

        df_dup = pd.DataFrame({'a': [1, 1, 2]})
        issue_dup = detect_duplicates(df_dup)[0]

        # Different fields!
        print(f"\nmissing_value: {list(issue_missing.keys())}")
        print(f"duplicate:     {list(issue_dup.keys())}")

        # missing_value returns row_indices, duplicate returns sample_indices
        assert 'row_indices' in issue_missing
        assert 'sample_indices' in issue_dup


class TestTestSuiteIssues:
    """The test suite is now invalid due to schema mismatch."""

    def test_old_test_suite_will_fail(self):
        """Original test_missing_value_detector.py tests check for old fields.

        With uncommitted refactor changes, those tests fail:
        - Expect 'missing_count', 'missing_pct' at root level
        - Detector now has different structure
        - suggest_strategy() function broken
        """
        # The test file still expects:
        # issue['missing_count']
        # issue['missing_pct']
        # issue['sample_values']
        #
        # But detector now returns these in different locations
        pass


class TestUncommittedChangesStatus:
    """Document the work-in-progress state."""

    def test_missing_value_detector_partially_refactored(self):
        """Someone started refactoring but didn't finish."""
        df = pd.DataFrame({'x': [1, None, 3]})
        issue = detect_missing(df)[0]

        # Has new fields:
        assert 'detector' in issue
        assert 'severity' in issue
        assert 'row_indices' in issue
        assert 'actions' in issue

        # But also has old fields:
        assert 'missing_count' in issue
        assert 'missing_pct' in issue
        assert 'sample_values' in issue

        # Inconsistency: 'column' should be 'columns'
        assert 'column' in issue
        assert 'columns' not in issue

    def test_orchestrator_py_partially_fixed(self):
        """Someone added 'explanation' → 'summary' conversion.

        This fixes part of the schema issue but:
        - Still returns 'column' instead of 'columns'
        - Other detectors haven't been updated
        """
        # Lines added to orchestrator:
        # for issue in explained_issues:
        #     issue['summary'] = issue.pop('explanation', issue.get('summary', ''))
        # This converts explanation_layer output to CLAUDE.md field name
        pass


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
