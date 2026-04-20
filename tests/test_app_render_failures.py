"""Test what breaks when app tries to render issues with incomplete schema."""
import pandas as pd
import pytest

from detectors.missing_value_detector import detect_missing
from detectors.duplicate_detector import detect as detect_duplicates


class TestAppRenderLogic:
    """Simulate app.py rendering logic with incomplete detector schemas."""

    def test_render_columns_line_98_fails_with_column(self):
        """app.py line 98 tries: ', '.join(issue['columns'])

        But duplicate_detector returns no 'columns' field.
        This will cause KeyError at render time.
        """
        df = pd.DataFrame({'a': [1, 1, 2], 'b': [1, 1, 3]})
        issues = detect_duplicates(df)

        if issues:
            issue = issues[0]
            # Simulate app line 98
            try:
                columns_str = ', '.join(issue['columns'])
                assert False, "Should have raised KeyError for 'columns'"
            except KeyError as e:
                # Expected - 'columns' not in issue
                assert "columns" in str(e)

    def test_render_actions_displays_nothing(self):
        """app.py line 103: actions = issue.get('actions', [])

        If no actions, app shows "No actions available for this issue."
        """
        df = pd.DataFrame({'a': [1, 1, 2], 'b': [1, 1, 3]})
        issues = detect_duplicates(df)

        if issues:
            issue = issues[0]
            # Simulate app line 103
            actions = issue.get('actions', [])
            assert len(actions) == 0, "Duplicate detector provides no actions"

    def test_missing_value_has_actions(self):
        """Only missing_value_detector has actions."""
        df = pd.DataFrame({'x': [1, None, 3]})
        issues = detect_missing(df)

        if issues:
            issue = issues[0]
            actions = issue.get('actions', [])
            assert len(actions) > 0

    def test_execute_action_missing_params_field(self):
        """app.py line 127: action_params = action.get('params', {})

        missing_value_detector provides 'params', but others don't.
        """
        df = pd.DataFrame({'x': [1, None, 3]})
        issues = detect_missing(df)

        if issues:
            issue = issues[0]
            for action in issue['actions']:
                # Simulate app line 127
                action_params = action.get('params', {})
                assert isinstance(action_params, dict)

    def test_render_metrics_row_indices_missing(self):
        """app.py line 100: len(issue.get('row_indices', []))

        duplicate_detector doesn't provide row_indices.
        """
        df = pd.DataFrame({'a': [1, 1, 2], 'b': [1, 1, 3]})
        issues = detect_duplicates(df)

        if issues:
            issue = issues[0]
            # Simulate app line 100
            rows_affected = len(issue.get('row_indices', []))
            assert rows_affected == 0, "duplicate_detector missing row_indices"


class TestEndToEndRenderFlow:
    """Full app render simulation."""

    def test_duplicate_detector_issue_render_simulation(self):
        """Simulate rendering duplicate detector issue."""
        df = pd.DataFrame({'a': [1, 1, 2], 'b': [1, 1, 3]})
        issues = detect_duplicates(df)

        if not issues:
            return

        issue = issues[0]
        print(f"\n=== Rendering duplicate detector issue ===")
        print(f"Fields: {list(issue.keys())}")

        # Line 96: severity
        severity = issue.get('severity', 'medium')
        print(f"Severity: {severity}")
        assert severity == 'medium', "Missing severity field"

        # Line 98: columns
        try:
            columns_str = ', '.join(issue['columns'])
            print(f"Columns: {columns_str}")
        except KeyError:
            print(f"ERROR: Missing 'columns' field (has: {list(issue.keys())})")

        # Line 100: rows affected
        rows_affected = len(issue.get('row_indices', []))
        print(f"Rows Affected: {rows_affected}")

        # Line 103: actions
        actions = issue.get('actions', [])
        if not actions:
            print("Message: No actions available for this issue.")
        else:
            for action in actions:
                print(f"  - {action.get('label')}: {action.get('description')}")

    def test_missing_value_issue_render_simulation(self):
        """Simulate rendering missing value detector issue."""
        df = pd.DataFrame({'age': [25, None, 35]})
        issues = detect_missing(df)

        if not issues:
            return

        issue = issues[0]
        print(f"\n=== Rendering missing value detector issue ===")
        print(f"Fields: {list(issue.keys())}")

        # Line 96: severity
        severity = issue.get('severity', 'medium')
        print(f"Severity: {severity}")
        assert severity in ('low', 'medium', 'high')

        # Line 98: columns - WILL FAIL
        try:
            columns_str = ', '.join(issue['columns'])
            print(f"Columns: {columns_str}")
        except KeyError:
            print(f"ERROR: Missing 'columns' field - has 'column' instead: {issue.get('column')}")

        # Line 100: rows affected
        rows_affected = len(issue.get('row_indices', []))
        print(f"Rows Affected: {rows_affected}")

        # Line 103: actions
        actions = issue.get('actions', [])
        if not actions:
            print("Message: No actions available.")
        else:
            for action in actions:
                print(f"  - {action.get('label')}: {action.get('description')}")


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
