"""Test schema compliance for each detector against CLAUDE.md requirement."""
import pandas as pd
import pytest

from detectors.missing_value_detector import detect_missing
from detectors.duplicate_detector import detect as detect_duplicates
from detectors.outlier_detector import detect as detect_outliers
from detectors.schema_analyzer import detect as detect_schema
from detectors.consistency_cleaner import detect as detect_consistency

# CLAUDE.md required schema:
# {
#     'detector': str,          # name of detector
#     'type': str,              # issue type (added by orchestrator)
#     'columns': list[str],     # affected column(s)  <-- APP EXPECTS LIST
#     'severity': str,          # 'low' | 'medium' | 'high'
#     'row_indices': list[int], # affected rows
#     'summary': str,           # plain-English explanation
#     'sample_data': dict,      # column-level stats
#     'actions': list[dict],    # [{id, label, description, params}, ...]
# }


class TestMissingValueDetectorSchema:
    """missing_value_detector schema compliance."""

    def test_returns_new_schema(self):
        """Verify detector returns most new fields."""
        df = pd.DataFrame({'x': [1, None, 3]})
        issues = detect_missing(df)
        assert len(issues) == 1
        issue = issues[0]

        # Has new fields
        assert 'detector' in issue
        assert issue['detector'] == 'missing_value_detector'
        assert 'severity' in issue
        assert issue['severity'] in ('low', 'medium', 'high')
        assert 'row_indices' in issue
        assert isinstance(issue['row_indices'], list)
        assert 'sample_data' in issue
        assert isinstance(issue['sample_data'], dict)
        assert 'actions' in issue
        assert isinstance(issue['actions'], list)

    def test_missing_type_at_source(self):
        """Type is added by orchestrator, not detector."""
        df = pd.DataFrame({'x': [1, None, 3]})
        issues = detect_missing(df)
        assert 'type' not in issues[0]  # Will be added by orchestrator

    def test_column_not_columns(self):
        """Detector returns 'column' (str) not 'columns' (list)."""
        df = pd.DataFrame({'age': [1, None, 3]})
        issues = detect_missing(df)
        assert 'column' in issues[0]
        assert 'columns' not in issues[0]  # ❌ APP EXPECTS 'columns'

    def test_actions_structure(self):
        """Actions have required fields."""
        df = pd.DataFrame({'x': [1, None, 3]})
        issues = detect_missing(df)
        actions = issues[0]['actions']
        assert len(actions) > 0

        for action in actions:
            assert 'id' in action
            assert 'label' in action
            assert 'description' in action
            assert 'params' in action


class TestDuplicateDetectorSchema:
    """duplicate_detector schema compliance - INCOMPLETE."""

    def test_missing_detector_field(self):
        """Missing 'detector' field."""
        df = pd.DataFrame({'a': [1, 1], 'b': [1, 1]})
        issues = detect_duplicates(df)
        if issues:
            assert 'detector' not in issues[0], "Missing 'detector' field"

    def test_missing_severity(self):
        """Missing 'severity' field."""
        df = pd.DataFrame({'a': [1, 1, 2], 'b': [1, 1, 3]})
        issues = detect_duplicates(df)
        if issues:
            assert 'severity' not in issues[0], "Missing 'severity' field"

    def test_missing_row_indices(self):
        """Missing 'row_indices' field."""
        df = pd.DataFrame({'a': [1, 1, 2], 'b': [1, 1, 3]})
        issues = detect_duplicates(df)
        if issues:
            # Current: has sample_indices, not row_indices
            assert 'row_indices' not in issues[0], "Missing 'row_indices' field"
            assert 'sample_indices' in issues[0]  # What it actually returns

    def test_missing_actions(self):
        """Missing 'actions' field."""
        df = pd.DataFrame({'a': [1, 1, 2], 'b': [1, 1, 3]})
        issues = detect_duplicates(df)
        if issues:
            assert 'actions' not in issues[0], "Missing 'actions' field"


class TestOutlierDetectorSchema:
    """outlier_detector schema compliance - INCOMPLETE."""

    def test_missing_detector_field(self):
        """Missing 'detector' field."""
        df = pd.DataFrame({'x': [1, 2, 3, 4, 100]})
        issues = detect_outliers(df)
        if issues:
            assert 'detector' not in issues[0], "Missing 'detector' field"

    def test_missing_severity(self):
        """Missing 'severity' field."""
        df = pd.DataFrame({'x': [1, 2, 3, 4, 100]})
        issues = detect_outliers(df)
        if issues:
            assert 'severity' not in issues[0], "Missing 'severity' field"

    def test_missing_actions(self):
        """Missing 'actions' field."""
        df = pd.DataFrame({'x': [1, 2, 3, 4, 100]})
        issues = detect_outliers(df)
        if issues:
            assert 'actions' not in issues[0], "Missing 'actions' field"

    def test_column_not_columns(self):
        """Returns 'column' (str) not 'columns' (list)."""
        df = pd.DataFrame({'x': [1, 2, 3, 4, 100]})
        issues = detect_outliers(df)
        if issues:
            assert 'column' in issues[0]
            assert 'columns' not in issues[0]


class TestSchemaAnalyzerSchema:
    """schema_analyzer schema compliance - INCOMPLETE."""

    def test_missing_detector_field(self):
        """Missing 'detector' field."""
        df = pd.DataFrame({'age': ['25', '30', '35']})
        issues = detect_schema(df)
        if issues:
            assert 'detector' not in issues[0], "Missing 'detector' field"

    def test_missing_severity(self):
        """Missing 'severity' field."""
        df = pd.DataFrame({'age': ['25', '30', '35']})
        issues = detect_schema(df)
        if issues:
            assert 'severity' not in issues[0], "Missing 'severity' field"

    def test_missing_row_indices(self):
        """Missing 'row_indices' field."""
        df = pd.DataFrame({'age': ['25', '30', '35']})
        issues = detect_schema(df)
        if issues:
            assert 'row_indices' not in issues[0], "Missing 'row_indices' field"

    def test_missing_actions(self):
        """Missing 'actions' field."""
        df = pd.DataFrame({'age': ['25', '30', '35']})
        issues = detect_schema(df)
        if issues:
            assert 'actions' not in issues[0], "Missing 'actions' field"


class TestConsistencyCleanerSchema:
    """consistency_cleaner schema compliance - INCOMPLETE."""

    def test_missing_detector_field(self):
        """Missing 'detector' field."""
        df = pd.DataFrame({'name': ['Alice ', ' Bob', 'alice']})
        issues = detect_consistency(df)
        if issues:
            assert 'detector' not in issues[0], "Missing 'detector' field"

    def test_missing_severity(self):
        """Missing 'severity' field."""
        df = pd.DataFrame({'name': ['Alice ', ' Bob']})
        issues = detect_consistency(df)
        if issues:
            assert 'severity' not in issues[0], "Missing 'severity' field"

    def test_missing_actions(self):
        """Missing 'actions' field."""
        df = pd.DataFrame({'name': ['Alice ', ' Bob']})
        issues = detect_consistency(df)
        if issues:
            assert 'actions' not in issues[0], "Missing 'actions' field"


class TestAppIntegrationConcerns:
    """Test what app.py actually expects."""

    def test_app_line_98_expects_columns_list(self):
        """app.py line 98: st.text(f"Columns: {', '.join(issue['columns'])}")

        This requires 'columns' to be a list of strings.
        All detectors return 'column' (singular string) instead.
        """
        df = pd.DataFrame({'age': [25, None, 35]})
        issues = detect_missing(df)
        if issues:
            issue = issues[0]
            # App does: ', '.join(issue['columns'])
            # But issue has 'column', not 'columns'
            with pytest.raises((KeyError, TypeError)):
                ', '.join(issue['columns'])  # Will fail

    def test_app_line_103_expects_actions(self):
        """app.py line 103: actions = issue.get('actions', [])

        If 'actions' is empty, app shows "No actions available".
        """
        df = pd.DataFrame({'a': [1, 1, 2], 'b': [1, 1, 3]})
        issues = detect_duplicates(df)
        if issues:
            issue = issues[0]
            actions = issue.get('actions', [])
            assert len(actions) == 0, "App will show 'No actions' message"

    def test_app_expects_summary_key(self):
        """app.py line 95: issue.get('summary', 'Data Quality Issue')"""
        df = pd.DataFrame({'age': [25, None, 35]})
        issues = detect_missing(df)
        if issues:
            # missing_value_detector has 'summary' but explanation_layer calls it 'explanation'
            assert 'summary' in issues[0] or 'explanation' not in issues[0]


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
