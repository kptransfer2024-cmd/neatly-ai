import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from orchestrator import run_diagnosis, _collect_df_stats


# --- _collect_df_stats tests ---

def test_collect_stats_empty_df():
    df = pd.DataFrame()
    stats = _collect_df_stats(df)
    assert stats['rows'] == 0
    assert stats['columns'] == 0


def test_collect_stats_single_numeric_column():
    df = pd.DataFrame({'a': [1.0, 2.0, 3.0]})
    stats = _collect_df_stats(df)
    assert stats['rows'] == 3
    assert stats['columns'] == 1
    assert 'a_stats' in stats
    assert stats['a_stats']['mean'] == 2.0
    assert stats['a_stats']['median'] == 2.0


def test_collect_stats_numeric_with_nulls():
    df = pd.DataFrame({'a': [1.0, None, 3.0]})
    stats = _collect_df_stats(df)
    assert stats['a_stats']['non_null_count'] == 2
    assert stats['a_stats']['null_count'] == 1


def test_collect_stats_categorical_column():
    df = pd.DataFrame({'color': ['red', 'blue', 'red', 'green']})
    stats = _collect_df_stats(df)
    assert stats['color_stats']['dtype'] in ('object', 'str')
    assert 'mode' in stats['color_stats']
    assert stats['color_stats']['mode'] == 'red'


def test_collect_stats_integer_column():
    df = pd.DataFrame({'count': [1, 2, 3, 4, 5]})
    stats = _collect_df_stats(df)
    assert stats['count_stats']['mean'] == 3.0
    assert stats['count_stats']['min'] == 1.0
    assert stats['count_stats']['max'] == 5.0


def test_collect_stats_multiple_columns():
    df = pd.DataFrame({
        'age': [25, 30, 35],
        'name': ['Alice', 'Bob', 'Charlie'],
        'score': [95.5, 87.3, 92.1]
    })
    stats = _collect_df_stats(df)
    assert stats['columns'] == 3
    assert 'age_stats' in stats
    assert 'name_stats' in stats
    assert 'score_stats' in stats


def test_collect_stats_mixed_dtypes():
    df = pd.DataFrame({
        'int_col': [1, 2, 3],
        'float_col': [1.1, 2.2, 3.3],
        'str_col': ['a', 'b', 'c'],
    })
    stats = _collect_df_stats(df)
    assert 'mean' in stats['int_col_stats']
    assert 'mean' in stats['float_col_stats']
    assert 'mode' in stats['str_col_stats']


# --- run_diagnosis tests ---

def test_run_diagnosis_returns_result():
    """Verify run_diagnosis returns a DiagnosisResult dict."""
    df = pd.DataFrame({'a': [1, 2, 3]})
    with patch('orchestrator.explain_issues', return_value=[]):
        result = run_diagnosis(df)
    assert isinstance(result, dict)
    assert 'issues' in result
    assert 'quality_score' in result
    assert 'diagnosed_at' in result
    assert 'row_count' in result
    assert 'column_count' in result


def test_run_diagnosis_returns_issues():
    """Verify run_diagnosis populates issues in the returned dict."""
    df = pd.DataFrame({'a': [1, None, 3]})
    with patch('orchestrator.explain_issues') as mock_explain:
        mock_explain.return_value = [{'type': 'missing_value', 'explanation': 'test', 'summary': 'test'}]
        result = run_diagnosis(df)
    assert len(result['issues']) > 0
    assert result['issues'][0]['summary'] == 'test'


def test_run_diagnosis_calls_all_detectors():
    """Verify run_diagnosis calls all 5 detectors."""
    df = pd.DataFrame({'a': [1, 2, 3], 'b': [1, 1, 2]})
    with patch('orchestrator.detect_missing') as mock_missing, \
         patch('orchestrator.detect_duplicates') as mock_dups, \
         patch('orchestrator.detect_schema') as mock_schema, \
         patch('orchestrator.detect_consistency') as mock_consistency, \
         patch('orchestrator.detect_outliers') as mock_outliers, \
         patch('orchestrator.explain_issues', return_value=[]):

        mock_missing.return_value = []
        mock_dups.return_value = []
        mock_schema.return_value = []
        mock_consistency.return_value = []
        mock_outliers.return_value = []

        run_diagnosis(df)

        mock_missing.assert_called_once()
        mock_dups.assert_called_once()
        mock_schema.assert_called_once()
        mock_consistency.assert_called_once()
        mock_outliers.assert_called_once()


def test_run_diagnosis_handles_detector_exception():
    """Verify run_diagnosis continues if a detector raises an exception."""
    df = pd.DataFrame({'a': [1, 2, 3]})
    with patch('orchestrator.detect_missing', side_effect=ValueError('Test error')), \
         patch('orchestrator.detect_duplicates', return_value=[]), \
         patch('orchestrator.detect_schema', return_value=[]), \
         patch('orchestrator.detect_consistency', return_value=[]), \
         patch('orchestrator.detect_outliers', return_value=[]), \
         patch('orchestrator.explain_issues', return_value=[]):

        result = run_diagnosis(df)
        assert result['row_count'] == 3


def test_run_diagnosis_injects_missing_type_key():
    """Verify run_diagnosis adds 'type' key when missing (e.g., from missing_value_detector)."""
    df = pd.DataFrame({'a': [1, None, 3]})
    with patch('orchestrator.detect_missing') as mock_missing, \
         patch('orchestrator.detect_duplicates', return_value=[]), \
         patch('orchestrator.detect_schema', return_value=[]), \
         patch('orchestrator.detect_consistency', return_value=[]), \
         patch('orchestrator.detect_outliers', return_value=[]), \
         patch('orchestrator.explain_issues') as mock_explain:

        mock_missing.return_value = [{'column': 'a', 'missing_count': 1}]
        mock_explain.return_value = [{'column': 'a', 'type': 'missing_value', 'explanation': 'test'}]

        run_diagnosis(df)

        called_issues = mock_explain.call_args[0][0]
        assert called_issues[0]['type'] == 'missing_value'


def test_run_diagnosis_preserves_existing_type():
    """Verify run_diagnosis doesn't override 'type' if already present."""
    df = pd.DataFrame({'a': [1, 1, 2]})
    with patch('orchestrator.detect_missing', return_value=[]), \
         patch('orchestrator.detect_duplicates') as mock_dups, \
         patch('orchestrator.detect_schema', return_value=[]), \
         patch('orchestrator.detect_consistency', return_value=[]), \
         patch('orchestrator.detect_outliers', return_value=[]), \
         patch('orchestrator.explain_issues') as mock_explain:

        mock_dups.return_value = [{'type': 'duplicates', 'duplicate_count': 1}]
        mock_explain.return_value = [{'type': 'duplicates', 'explanation': 'test'}]

        run_diagnosis(df)

        called_issues = mock_explain.call_args[0][0]
        assert called_issues[0]['type'] == 'duplicates'


def test_run_diagnosis_passes_stats_to_explain_issues():
    """Verify run_diagnosis passes df_stats to explain_issues."""
    df = pd.DataFrame({'a': [1, 2, 3]})
    with patch('orchestrator.detect_missing', return_value=[]), \
         patch('orchestrator.detect_duplicates', return_value=[]), \
         patch('orchestrator.detect_schema', return_value=[]), \
         patch('orchestrator.detect_consistency', return_value=[]), \
         patch('orchestrator.detect_outliers', return_value=[]), \
         patch('orchestrator.explain_issues') as mock_explain:

        mock_explain.return_value = []
        run_diagnosis(df)

        called_stats = mock_explain.call_args[0][1]
        assert 'rows' in called_stats
        assert 'columns' in called_stats
        assert 'a_stats' in called_stats


def test_run_diagnosis_multiple_issues_per_detector():
    """Verify run_diagnosis handles multiple issues from one detector."""
    df = pd.DataFrame({'a': [1, None, 3], 'b': ['x', 'x', None]})
    with patch('orchestrator.detect_missing') as mock_missing, \
         patch('orchestrator.detect_duplicates', return_value=[]), \
         patch('orchestrator.detect_schema', return_value=[]), \
         patch('orchestrator.detect_consistency', return_value=[]), \
         patch('orchestrator.detect_outliers', return_value=[]), \
         patch('orchestrator.explain_issues') as mock_explain:

        mock_missing.return_value = [
            {'column': 'a', 'missing_count': 1},
            {'column': 'b', 'missing_count': 1},
        ]
        mock_explain.return_value = [
            {'type': 'missing_value', 'column': 'a', 'explanation': 'test1'},
            {'type': 'missing_value', 'column': 'b', 'explanation': 'test2'},
        ]

        run_diagnosis(df)

        called_issues = mock_explain.call_args[0][0]
        assert len(called_issues) == 2
        assert called_issues[0]['type'] == 'missing_value'
        assert called_issues[1]['type'] == 'missing_value'


def test_run_diagnosis_empty_detector_results():
    """Verify run_diagnosis handles empty results from all detectors."""
    df = pd.DataFrame({'a': [1, 2, 3]})
    with patch('orchestrator.detect_missing', return_value=[]), \
         patch('orchestrator.detect_duplicates', return_value=[]), \
         patch('orchestrator.detect_schema', return_value=[]), \
         patch('orchestrator.detect_consistency', return_value=[]), \
         patch('orchestrator.detect_outliers', return_value=[]), \
         patch('orchestrator.explain_issues', return_value=[]):

        result = run_diagnosis(df)

        assert result['issues'] == []
        assert 'quality_score' in result
        assert result['row_count'] == 3


def test_run_diagnosis_empty_dataframe():
    """Verify run_diagnosis handles empty DataFrame gracefully."""
    df = pd.DataFrame()
    with patch('orchestrator.detect_missing', return_value=[]), \
         patch('orchestrator.detect_duplicates', return_value=[]), \
         patch('orchestrator.detect_schema', return_value=[]), \
         patch('orchestrator.detect_consistency', return_value=[]), \
         patch('orchestrator.detect_outliers', return_value=[]), \
         patch('orchestrator.explain_issues', return_value=[]):

        result = run_diagnosis(df)
        assert result['row_count'] == 0
        assert result['column_count'] == 0


# --- Integration test ---

def test_run_diagnosis_full_flow():
    """Integration test: run_diagnosis with realistic data and mocked explanations."""
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, None, 35],
        'score': [95.5, 87.3, 95.5],
    })

    with patch('orchestrator.explain_issues') as mock_explain:
        mock_explain.return_value = [
            {
                'type': 'missing_value',
                'column': 'age',
                'missing_count': 1,
                'explanation': 'The age column has 1 missing value (33.3%).',
                'summary': 'The age column has 1 missing value (33.3%).',
            },
            {
                'type': 'duplicates',
                'duplicate_count': 1,
                'explanation': 'One row is a duplicate.',
                'summary': 'One row is a duplicate.',
            },
        ]

        result = run_diagnosis(df)

        assert len(result['issues']) == 2
        assert result['issues'][0]['type'] == 'missing_value'
        assert result['issues'][1]['type'] == 'duplicates'
        assert 'quality_score' in result
        assert result['row_count'] == 3
