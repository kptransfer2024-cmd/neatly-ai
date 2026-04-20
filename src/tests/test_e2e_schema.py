"""End-to-end schema validation: detector → explanation → app."""
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
import streamlit as st

from detectors.missing_value_detector import detect_missing
from detectors.duplicate_detector import detect as detect_duplicates
from detectors.outlier_detector import detect as detect_outliers
from detectors.schema_analyzer import detect as detect_schema
from detectors.consistency_cleaner import detect as detect_consistency
from orchestrator import run_diagnosis

_REQUIRED_FIELDS = ('detector', 'severity', 'row_indices', 'actions')


def test_missing_value_detector_has_required_fields():
    """Verify missing_value_detector (before orchestrator) has required fields."""
    df = pd.DataFrame({'age': [25, None, 35]})
    issues = detect_missing(df)

    assert len(issues) == 1
    issue = issues[0]

    # Detector-level required fields (type is added by orchestrator)
    required = {'detector', 'columns', 'severity', 'row_indices',
                'summary', 'sample_data', 'actions'}
    assert required.issubset(set(issue.keys())), f"Missing: {required - set(issue.keys())}"

    # Validate field types
    assert isinstance(issue['detector'], str)
    assert isinstance(issue['columns'], list)
    assert issue['severity'] in ('low', 'medium', 'high')
    assert isinstance(issue['row_indices'], list)
    assert isinstance(issue['summary'], str)
    assert isinstance(issue['sample_data'], dict)
    assert isinstance(issue['actions'], list)

    # Validate actions structure
    for action in issue['actions']:
        assert 'id' in action
        assert 'label' in action
        assert 'description' in action
        assert 'params' in action


def test_duplicate_detector_has_required_fields():
    """Verify duplicate_detector returns full CLAUDE.md schema."""
    df = pd.DataFrame({'a': [1, 1, 2], 'b': [1, 1, 3]})
    issues = detect_duplicates(df)

    assert issues, 'duplicate_detector should return an issue for this input'
    issue = issues[0]
    missing = [f for f in _REQUIRED_FIELDS if f not in issue]
    assert not missing, f'duplicate_detector is missing fields: {missing}'


def test_outlier_detector_has_required_fields():
    """Verify outlier_detector returns full CLAUDE.md schema."""
    df = pd.DataFrame({'values': [1, 2, 3, 4, 100]})
    issues = detect_outliers(df)

    assert issues, 'outlier_detector should return an issue for this input'
    issue = issues[0]
    missing = [f for f in _REQUIRED_FIELDS if f not in issue]
    assert not missing, f'outlier_detector is missing fields: {missing}'


def test_schema_analyzer_has_required_fields():
    """Verify schema_analyzer returns full CLAUDE.md schema."""
    df = pd.DataFrame({'age': ['25', '30', '35']})
    issues = detect_schema(df)

    assert issues, 'schema_analyzer should return an issue for this input'
    issue = issues[0]
    missing = [f for f in _REQUIRED_FIELDS if f not in issue]
    assert not missing, f'schema_analyzer is missing fields: {missing}'


def test_app_expects_columns_as_list():
    """Verify detectors provide 'columns' (list) as app.py requires."""
    df = pd.DataFrame({'age': [25, None, 35]})
    issues = detect_missing(df)

    assert issues, 'missing_value_detector should return an issue for this input'
    issue = issues[0]
    assert 'columns' in issue, "detector must provide 'columns' (list)"
    assert isinstance(issue['columns'], list), "'columns' must be a list"
    assert 'column' not in issue, "detector should not expose the legacy 'column' (str) field"


def test_end_to_end_with_missing_value():
    """Full pipeline: detect → explain → render."""
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'age': [25, None, 35],
    })

    st.session_state['issues'] = []
    st.session_state['stage'] = 'upload'

    with patch('orchestrator.explain_issues') as mock_explain:
        # Mock should return the full schema
        def add_explanation(issues, stats):
            for issue in issues:
                issue['explanation'] = f"Issue in {issue.get('column', 'unknown')}"
            return issues

        mock_explain.side_effect = add_explanation
        run_diagnosis(df)

    issues = st.session_state['issues']
    if issues:
        issue = issues[0]
        # Check what fields are present
        print(f"\nEnd-to-end issue fields: {issue.keys()}")
        print(f"Issue: {issue}")


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
