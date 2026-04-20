from unittest.mock import MagicMock

import explanation_layer
from explanation_layer import (
    _build_issue_prompt,
    _build_stats_summary,
    _fallback_explanation,
    explain_issues,
)


class _FakeBlock:
    type = 'text'

    def __init__(self, text: str):
        self.text = text


class _FakeResponse:
    def __init__(self, text: str):
        self.content = [_FakeBlock(text)]


def _make_client(reply: str = 'The column contains missing values that may skew analysis.'):
    client = MagicMock()
    client.messages.create.return_value = _FakeResponse(reply)
    return client


# --- explain_issues ---

def test_empty_issues_returns_empty():
    assert explain_issues([], {}) == []


def test_attaches_explanation_key():
    client = _make_client('Some explanation.')
    issues = [{'type': 'missing', 'column': 'age', 'missing_count': 10}]
    result = explain_issues(issues, {}, client=client)
    assert result[0]['explanation'] == 'Some explanation.'


def test_one_api_call_per_issue():
    client = _make_client('x')
    issues = [
        {'type': 'missing', 'column': 'a'},
        {'type': 'duplicates'},
        {'type': 'outliers', 'column': 'b'},
    ]
    explain_issues(issues, {}, client=client)
    assert client.messages.create.call_count == 3


def test_mutates_and_returns_same_list():
    client = _make_client('x')
    issues = [{'type': 'missing', 'column': 'a'}]
    result = explain_issues(issues, {}, client=client)
    assert result is issues
    assert 'explanation' in issues[0]


def test_raw_values_not_sent_to_api():
    client = _make_client('ok')
    issues = [{
        'type': 'mixed_case',
        'column': 'fruit',
        'example_values': ['apple', 'Apple', 'APPLE'],
        'sample_indices': [1, 2, 3],
        'row_indices': [7, 8],
    }]
    explain_issues(issues, {}, client=client)
    prompt = client.messages.create.call_args.kwargs['messages'][0]['content']
    assert 'apple' not in prompt.lower()
    assert '[1, 2, 3]' not in prompt
    assert '[7, 8]' not in prompt
    assert 'fruit' in prompt  # column name is allowed


def test_stats_summary_included_in_prompt():
    client = _make_client('ok')
    issues = [{'type': 'missing', 'column': 'x'}]
    explain_issues(issues, {'row_count': 1000, 'col_count': 5}, client=client)
    prompt = client.messages.create.call_args.kwargs['messages'][0]['content']
    assert '1000' in prompt
    assert 'row_count' in prompt


def test_correct_model_and_system_prompt():
    client = _make_client('ok')
    explain_issues([{'type': 'missing', 'column': 'x'}], {}, client=client)
    call = client.messages.create.call_args
    assert call.kwargs['model'] == 'claude-sonnet-4-6'
    assert 'data-quality assistant' in call.kwargs['system']


def test_api_error_falls_back_gracefully():
    class _StubAPIError(explanation_layer.anthropic.APIError):
        def __init__(self):
            pass  # bypass parent init; only need the isinstance match

    client = MagicMock()
    client.messages.create.side_effect = _StubAPIError()
    issues = [{'type': 'missing', 'column': 'age'}]
    result = explain_issues(issues, {}, client=client)
    assert 'missing' in result[0]['explanation']
    assert 'age' in result[0]['explanation']


def test_empty_api_response_uses_fallback():
    client = _make_client('')  # whitespace-only / empty
    issues = [{'type': 'outliers', 'column': 'price'}]
    result = explain_issues(issues, {}, client=client)
    assert 'outliers' in result[0]['explanation']
    assert 'price' in result[0]['explanation']


# --- helpers ---

def test_build_stats_summary_empty():
    assert _build_stats_summary({}) == ''


def test_build_stats_summary_formats_dict():
    result = _build_stats_summary({'rows': 100, 'cols': 5})
    assert '- rows: 100' in result
    assert '- cols: 5' in result


def test_build_issue_prompt_strips_unsafe_keys():
    prompt = _build_issue_prompt(
        {'type': 'x', 'column': 'c', 'example_values': ['a', 'b'], 'count': 7},
        stats_summary='',
    )
    assert 'example_values' not in prompt
    assert "['a', 'b']" not in prompt
    assert 'count: 7' in prompt


def test_build_issue_prompt_includes_stats_section():
    prompt = _build_issue_prompt(
        {'type': 'x', 'column': 'c'},
        stats_summary='- rows: 10',
    )
    assert 'Dataset context:' in prompt
    assert '- rows: 10' in prompt


def test_fallback_with_column():
    assert _fallback_explanation({'type': 'missing', 'column': 'age'}) == (
        "Detected missing in column 'age'."
    )


def test_fallback_without_column():
    assert _fallback_explanation({'type': 'duplicates'}) == 'Detected duplicates.'
