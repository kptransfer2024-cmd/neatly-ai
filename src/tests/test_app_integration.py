"""Integration tests simulating actual app usage and potential crashes."""
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
import streamlit as st

if 'issues' not in st.session_state:
    st.session_state['issues'] = []
if 'stage' not in st.session_state:
    st.session_state['stage'] = 'upload'

from orchestrator import run_diagnosis
from transformation_executor import (
    drop_duplicates, fill_missing, drop_missing, clip_outliers, cast_column, normalize_text
)


class TestActualAppDataFlow:
    """Test the actual data flow a user would experience."""

    def test_user_uploads_csv_with_missing_values(self):
        """User flow: upload -> diagnose -> decide -> apply fix -> done"""
        # Step 1: Upload
        df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', None, 'David', 'Eve'],
            'score': [95.5, 87.0, 92.3, None, 88.5],
        })

        st.session_state['df'] = df
        st.session_state['original_df'] = df.copy()
        st.session_state['cleaning_log'] = []

        # Step 2: Diagnose
        with patch('orchestrator.explain_issues') as mock_explain:
            # Mock the explanation layer to add explanations
            def mock_explain_fn(issues, stats):
                for issue in issues:
                    issue['explanation'] = f"Issue: {issue.get('type', 'unknown')}"
                return issues

            mock_explain.side_effect = mock_explain_fn
            run_diagnosis(st.session_state['df'])

        issues = st.session_state['issues']
        print(f"\nFound {len(issues)} issues")

        # Step 3: Try to render issues (THIS IS WHERE IT CRASHES)
        if issues:
            for i, issue in enumerate(issues):
                print(f"\nIssue {i}:")
                print(f"  Type: {issue.get('type')}")
                print(f"  Detector: {issue.get('detector')}")

                # THIS LINE CRASHES IN REAL APP (line 98)
                try:
                    columns = ', '.join(issue['columns'])
                    print(f"  Columns: {columns}")
                except KeyError as e:
                    print(f"  [FAIL] CRASH: Missing 'columns' field: {e}")
                    print(f"     Has 'column' instead: {issue.get('column')}")

                # Try to render actions (line 103)
                actions = issue.get('actions', [])
                if not actions:
                    print(f"  [WARN]  No actions available for this issue")
                else:
                    for action in actions:
                        print(f"    - {action.get('label')}")

    def test_transformation_executor_works_with_valid_params(self):
        """Test that transformation_executor works correctly."""
        df = pd.DataFrame({
            'age': [25, None, 35, 40],
            'name': ['Alice', 'Bob', 'Charlie', 'David'],
        })

        cleaning_log = []

        # Test fill_missing
        df_filled = fill_missing(df, 'age', 'median', cleaning_log)
        assert df_filled['age'].isna().sum() == 0
        assert len(cleaning_log) == 1

        # Test drop_missing
        df_dropped = drop_missing(df, 'age', cleaning_log)
        assert len(df_dropped) < len(df)
        assert len(cleaning_log) == 2

        # Test normalize_text
        df_norm = normalize_text(df, 'name', 'lowercase', cleaning_log)
        assert all(x.islower() for x in df_norm['name'] if pd.notna(x))
        assert len(cleaning_log) == 3

    def test_missing_value_detector_provides_valid_params(self):
        """Verify missing_value_detector provides valid params for transformation_executor."""
        from detectors.missing_value_detector import detect_missing

        df = pd.DataFrame({'age': [25, None, 35]})
        issues = detect_missing(df)

        assert len(issues) == 1
        issue = issues[0]
        actions = issue.get('actions', [])
        assert len(actions) > 0

        # Extract params and try to apply them
        cleaning_log = []
        for action in actions:
            action_id = action.get('id')
            params = action.get('params', {})

            print(f"\nAction: {action_id}")
            print(f"Params: {params}")

            if action_id == 'fill_missing':
                # Should have column and strategy
                assert 'column' in params
                assert 'strategy' in params
                # Try to apply
                result = fill_missing(df, **params, cleaning_log=cleaning_log)
                assert isinstance(result, pd.DataFrame)

            elif action_id == 'drop_missing':
                # Should have columns
                assert 'columns' in params or 'column' in params
                # Note: drop_missing signature expects column not columns
                # This might be a bug
                print(f"  WARNING: Action uses 'columns' but transformation_executor expects 'column'")


class TestDetectorParamConsistency:
    """Test if detector params match transformation_executor signatures."""

    def test_fill_missing_params(self):
        """missing_value_detector provides params for fill_missing."""
        from detectors.missing_value_detector import detect_missing

        df = pd.DataFrame({'x': [1, None, 3]})
        issues = detect_missing(df)
        actions = issues[0]['actions']

        fill_action = [a for a in actions if a['id'] == 'fill_missing'][0]
        params = fill_action['params']

        # fill_missing signature: (df, column, strategy, cleaning_log, fill_value=None)
        assert 'column' in params
        assert 'strategy' in params
        print(f"[OK] fill_missing params valid: {params}")

    def test_drop_missing_param_mismatch(self):
        """missing_value_detector uses 'columns' but transformer expects 'column'."""
        from detectors.missing_value_detector import detect_missing

        df = pd.DataFrame({'x': [1, None, 3]})
        issues = detect_missing(df)
        actions = issues[0]['actions']

        drop_action = [a for a in actions if a['id'] == 'drop_missing'][0]
        params = drop_action['params']

        # Detector provides: {'columns': [col]}
        # But transformation_executor.drop_missing() signature: (df, column, cleaning_log)
        # MISMATCH!
        print(f"\n[FAIL] PARAM MISMATCH:")
        print(f"   Detector provides: {params}")
        print(f"   Expected by transformer: column (singular), not columns (list)")

        if 'columns' in params:
            print(f"   Bug: Should be 'column' not 'columns'")


class TestExplanationLayerIntegration:
    """Test explanation_layer interaction with detectors."""

    def test_explanation_layer_adds_explanation_key(self):
        """explanation_layer adds 'explanation' key (template-based, no API)."""
        from detectors.missing_value_detector import detect_missing
        from explanation_layer import explain_issues

        df = pd.DataFrame({'age': [25, None, 35]})
        issues = detect_missing(df)

        assert 'explanation' not in issues[0]

        explained = explain_issues(issues, {})

        assert 'explanation' in explained[0]
        assert explained[0]['explanation'] != ''
        print(f"[OK] explanation_layer adds 'explanation': {explained[0]['explanation']}")

    def test_orchestrator_converts_explanation_to_summary(self):
        """orchestrator.py converts 'explanation' to 'summary'."""
        # The new code in orchestrator does:
        # issue['summary'] = issue.pop('explanation', issue.get('summary', ''))
        # This converts the explanation_layer output to CLAUDE.md field name

        issue = {
            'type': 'missing',
            'column': 'age',
            'explanation': 'The age column has missing values.',
        }

        # Simulate what orchestrator does
        issue['summary'] = issue.pop('explanation', issue.get('summary', ''))

        assert 'explanation' not in issue
        assert 'summary' in issue
        assert issue['summary'] == 'The age column has missing values.'
        print(f"[OK] orchestrator converts explanation -> summary: {issue['summary']}")


class TestEdgeCasesAndErrors:
    """Test edge cases that might cause runtime errors."""

    def test_empty_dataframe_flow(self):
        """Empty DataFrame should not crash."""
        df = pd.DataFrame()

        st.session_state['df'] = df
        st.session_state['issues'] = []

        with patch('orchestrator.explain_issues', return_value=[]):
            run_diagnosis(df)

        assert st.session_state['issues'] == []
        print("[OK] Empty DataFrame handled gracefully")

    def test_all_null_column_flow(self):
        """All-null column should be handled."""
        df = pd.DataFrame({'x': [None, None, None]})

        st.session_state['df'] = df
        st.session_state['issues'] = []

        with patch('orchestrator.explain_issues') as mock_explain:
            def mock_fn(issues, stats):
                for issue in issues:
                    issue['explanation'] = 'All null'
                return issues

            mock_explain.side_effect = mock_fn
            run_diagnosis(df)

        issues = st.session_state['issues']
        if issues:
            print(f"[OK] All-null column detected: {issues[0].get('type')}")

    def test_single_row_dataframe(self):
        """Single row DataFrame should not crash."""
        df = pd.DataFrame({'a': [1], 'b': ['x']})

        st.session_state['df'] = df

        with patch('orchestrator.explain_issues', return_value=[]):
            run_diagnosis(df)

        print("[OK] Single-row DataFrame handled")

    def test_very_wide_dataframe(self):
        """Many columns should not cause performance issues."""
        # Create 100 columns
        data = {f'col_{i}': [1, 2, 3] for i in range(100)}
        df = pd.DataFrame(data)

        st.session_state['df'] = df

        with patch('orchestrator.explain_issues', return_value=[]):
            run_diagnosis(df)

        print("[OK] 100-column DataFrame processed")

    def test_very_tall_dataframe(self):
        """Many rows should not cause memory issues."""
        # Create 10k rows
        df = pd.DataFrame({
            'id': range(10000),
            'value': [i % 100 for i in range(10000)],
        })

        st.session_state['df'] = df

        with patch('orchestrator.explain_issues', return_value=[]):
            run_diagnosis(df)

        print("[OK] 10k-row DataFrame processed")


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
