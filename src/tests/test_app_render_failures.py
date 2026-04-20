"""Test what breaks when app tries to render issues with incomplete schema."""
import pandas as pd
import pytest

from detectors.missing_value_detector import detect_missing
from detectors.duplicate_detector import detect as detect_duplicates


class TestAppRenderLogic:
    """Simulate app.py rendering logic with incomplete detector schemas."""

    def test_render_columns_line_98_with_columns_list(self):
        """app.py line 98 tries: ', '.join(issue['columns'])

        All detectors now return 'columns' field as a list.
        This should work correctly without KeyError.
        """
        df = pd.DataFrame({'a': [1, 1, 2], 'b': [1, 1, 3]})
        issues = detect_duplicates(df)

        if issues:
            issue = issues[0]
            # Simulate app line 98
            assert 'columns' in issue, "Detector should have 'columns' field"
            assert isinstance(issue['columns'], list), "'columns' should be a list"
            columns_str = ', '.join(issue['columns'])
            assert isinstance(columns_str, str)

    def test_render_actions_available(self):
        """app.py line 103: actions = issue.get('actions', [])

        duplicate_detector now provides actions for the drop_duplicates operation.
        """
        df = pd.DataFrame({'a': [1, 1, 2], 'b': [1, 1, 3]})
        issues = detect_duplicates(df)

        if issues:
            issue = issues[0]
            # Simulate app line 103
            actions = issue.get('actions', [])
            assert len(actions) > 0, "Duplicate detector should provide actions"
            assert all('id' in a and 'label' in a for a in actions)

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

    def test_render_metrics_row_indices_present(self):
        """app.py line 100: len(issue.get('row_indices', []))

        duplicate_detector now provides row_indices for all detected duplicates.
        """
        df = pd.DataFrame({'a': [1, 1, 2], 'b': [1, 1, 3]})
        issues = detect_duplicates(df)

        if issues:
            issue = issues[0]
            # Simulate app line 100
            assert 'row_indices' in issue, "Should have row_indices field"
            rows_affected = len(issue.get('row_indices', []))
            assert rows_affected > 0, "duplicate_detector should provide row_indices"


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
        assert severity in ('low', 'medium', 'high'), "Should have valid severity"

        # Line 98: columns - should work now
        assert 'columns' in issue, "Should have 'columns' field"
        columns_str = ', '.join(issue['columns'])
        print(f"Columns: {columns_str}")
        assert isinstance(columns_str, str)

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

        # Line 98: columns - should work now
        assert 'columns' in issue, "Should have 'columns' field"
        columns_str = ', '.join(issue['columns'])
        print(f"Columns: {columns_str}")
        assert isinstance(columns_str, str)

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
