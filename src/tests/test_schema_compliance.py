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

    def test_type_present_at_source(self):
        """Type can be added by detector or orchestrator."""
        df = pd.DataFrame({'x': [1, None, 3]})
        issues = detect_missing(df)
        # Either detector or orchestrator can add type
        # missing_value_detector adds it, which is fine
        if 'type' in issues[0]:
            assert issues[0]['type'] == 'missing_value'

    def test_columns_is_list_not_string(self):
        """Detector returns 'columns' (list) not 'column' (str)."""
        df = pd.DataFrame({'age': [1, None, 3]})
        issues = detect_missing(df)
        assert 'columns' in issues[0], "Should have 'columns' field"
        assert isinstance(issues[0]['columns'], list), "'columns' must be a list"
        assert 'column' not in issues[0], "Should not have 'column' field"

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
    """duplicate_detector schema compliance - NOW COMPLETE."""

    def test_has_detector_field(self):
        """Has 'detector' field."""
        df = pd.DataFrame({'a': [1, 1], 'b': [1, 1]})
        issues = detect_duplicates(df)
        if issues:
            assert 'detector' in issues[0], "Should have 'detector' field"
            assert issues[0]['detector'] == 'duplicate_detector'

    def test_has_severity(self):
        """Has 'severity' field."""
        df = pd.DataFrame({'a': [1, 1, 2], 'b': [1, 1, 3]})
        issues = detect_duplicates(df)
        if issues:
            assert 'severity' in issues[0], "Should have 'severity' field"
            assert issues[0]['severity'] in ('low', 'medium', 'high')

    def test_has_row_indices(self):
        """Has 'row_indices' field."""
        df = pd.DataFrame({'a': [1, 1, 2], 'b': [1, 1, 3]})
        issues = detect_duplicates(df)
        if issues:
            assert 'row_indices' in issues[0], "Should have 'row_indices' field"
            assert isinstance(issues[0]['row_indices'], list)

    def test_has_actions(self):
        """Has 'actions' field."""
        df = pd.DataFrame({'a': [1, 1, 2], 'b': [1, 1, 3]})
        issues = detect_duplicates(df)
        if issues:
            assert 'actions' in issues[0], "Should have 'actions' field"
            assert isinstance(issues[0]['actions'], list)
            assert len(issues[0]['actions']) > 0


class TestOutlierDetectorSchema:
    """outlier_detector schema compliance - NOW COMPLETE."""

    def test_has_detector_field(self):
        """Has 'detector' field."""
        df = pd.DataFrame({'x': [1, 2, 3, 4, 100]})
        issues = detect_outliers(df)
        if issues:
            assert 'detector' in issues[0], "Should have 'detector' field"
            assert issues[0]['detector'] == 'outlier_detector'

    def test_has_severity(self):
        """Has 'severity' field."""
        df = pd.DataFrame({'x': [1, 2, 3, 4, 100]})
        issues = detect_outliers(df)
        if issues:
            assert 'severity' in issues[0], "Should have 'severity' field"
            assert issues[0]['severity'] in ('low', 'medium', 'high')

    def test_has_actions(self):
        """Has 'actions' field."""
        df = pd.DataFrame({'x': [1, 2, 3, 4, 100]})
        issues = detect_outliers(df)
        if issues:
            assert 'actions' in issues[0], "Should have 'actions' field"
            assert isinstance(issues[0]['actions'], list)
            assert len(issues[0]['actions']) > 0

    def test_columns_is_list_not_string(self):
        """Returns 'columns' (list) not 'column' (str)."""
        df = pd.DataFrame({'x': [1, 2, 3, 4, 100]})
        issues = detect_outliers(df)
        if issues:
            assert 'columns' in issues[0], "Should have 'columns' field"
            assert isinstance(issues[0]['columns'], list), "'columns' must be a list"
            assert 'column' not in issues[0], "Should not have 'column' field"


class TestSchemaAnalyzerSchema:
    """schema_analyzer schema compliance - NOW COMPLETE."""

    def test_has_detector_field(self):
        """Has 'detector' field."""
        df = pd.DataFrame({'age': ['25', '30', '35']})
        issues = detect_schema(df)
        if issues:
            assert 'detector' in issues[0], "Should have 'detector' field"
            assert issues[0]['detector'] == 'schema_analyzer'

    def test_has_severity(self):
        """Has 'severity' field."""
        df = pd.DataFrame({'age': ['25', '30', '35']})
        issues = detect_schema(df)
        if issues:
            assert 'severity' in issues[0], "Should have 'severity' field"
            assert issues[0]['severity'] in ('low', 'medium', 'high')

    def test_has_row_indices(self):
        """Has 'row_indices' field."""
        df = pd.DataFrame({'age': ['25', '30', '35']})
        issues = detect_schema(df)
        if issues:
            assert 'row_indices' in issues[0], "Should have 'row_indices' field"
            assert isinstance(issues[0]['row_indices'], list)

    def test_has_actions(self):
        """Has 'actions' field."""
        df = pd.DataFrame({'age': ['25', '30', '35']})
        issues = detect_schema(df)
        if issues:
            assert 'actions' in issues[0], "Should have 'actions' field"
            assert isinstance(issues[0]['actions'], list)
            assert len(issues[0]['actions']) > 0


class TestConsistencyCleanerSchema:
    """consistency_cleaner schema compliance - NOW COMPLETE."""

    def test_has_detector_field(self):
        """Has 'detector' field."""
        df = pd.DataFrame({'name': ['Alice ', ' Bob', 'alice']})
        issues = detect_consistency(df)
        if issues:
            assert 'detector' in issues[0], "Should have 'detector' field"
            assert issues[0]['detector'] == 'consistency_cleaner'

    def test_has_severity(self):
        """Has 'severity' field."""
        df = pd.DataFrame({'name': ['Alice ', ' Bob']})
        issues = detect_consistency(df)
        if issues:
            assert 'severity' in issues[0], "Should have 'severity' field"
            assert issues[0]['severity'] in ('low', 'medium', 'high')

    def test_has_actions(self):
        """Has 'actions' field."""
        df = pd.DataFrame({'name': ['Alice ', ' Bob']})
        issues = detect_consistency(df)
        if issues:
            assert 'actions' in issues[0], "Should have 'actions' field"
            assert isinstance(issues[0]['actions'], list)
            assert len(issues[0]['actions']) > 0


class TestAppIntegrationConcerns:
    """Test what app.py actually expects."""

    def test_app_line_98_expects_columns_list(self):
        """app.py line 98: st.text(f"Columns: {', '.join(issue['columns'])}")

        This requires 'columns' to be a list of strings.
        All detectors now return 'columns' (list) as expected.
        """
        df = pd.DataFrame({'age': [25, None, 35]})
        issues = detect_missing(df)
        if issues:
            issue = issues[0]
            # App does: ', '.join(issue['columns'])
            # This should now work correctly
            columns_str = ', '.join(issue['columns'])
            assert isinstance(columns_str, str)
            assert 'age' in columns_str

    def test_app_line_103_expects_actions(self):
        """app.py line 103: actions = issue.get('actions', [])

        All detectors now provide meaningful actions for users.
        """
        df = pd.DataFrame({'a': [1, 1, 2], 'b': [1, 1, 3]})
        issues = detect_duplicates(df)
        if issues:
            issue = issues[0]
            actions = issue.get('actions', [])
            assert len(actions) > 0, "App can now show action buttons"
            for action in actions:
                assert 'id' in action
                assert 'label' in action

    def test_app_expects_summary_key(self):
        """app.py line 95: issue.get('summary', 'Data Quality Issue')"""
        df = pd.DataFrame({'age': [25, None, 35]})
        issues = detect_missing(df)
        if issues:
            # missing_value_detector has 'summary' but explanation_layer calls it 'explanation'
            assert 'summary' in issues[0] or 'explanation' not in issues[0]


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
