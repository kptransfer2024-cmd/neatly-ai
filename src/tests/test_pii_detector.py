"""Test suite for PII detector."""
import pandas as pd
import pytest
from detectors.pii_detector import detect


class TestEmailDetection:
    """Test email address detection."""

    def test_valid_emails_high_confidence(self):
        """Detect valid email addresses with >60% match rate."""
        df = pd.DataFrame({
            'email': ['alice@example.com', 'bob@site.org', 'charlie@domain.co.uk', 'david@test.io']
        })
        issues = detect(df)
        assert len(issues) == 1
        assert issues[0]['type'] == 'pii_detected'
        assert issues[0]['columns'] == ['email']
        assert issues[0]['sample_data']['email']['pii_type'] == 'email'
        assert issues[0]['sample_data']['email']['match_rate'] == 100.0
        assert issues[0]['severity'] == 'high'

    def test_emails_mixed_valid_invalid(self):
        """Detect emails when >60% match (70% valid, 30% invalid)."""
        df = pd.DataFrame({
            'email': ['alice@example.com', 'bob@site.org', 'invalid-email', 'charlie@domain.com', None]
        })
        issues = detect(df)
        assert len(issues) == 1
        assert issues[0]['sample_data']['email']['pii_type'] == 'email'
        assert issues[0]['sample_data']['email']['match_rate'] > 60.0

    def test_emails_below_threshold(self):
        """Fallback to name heuristic when <60% pattern match."""
        df = pd.DataFrame({
            'email': ['alice@example.com', 'not-an-email', 'also-not', 'random-text', 'bob@site.org']
        })
        issues = detect(df)
        # Pattern match is 40%, but 'email' column name triggers heuristic
        assert len(issues) == 1
        assert issues[0]['sample_data']['email']['detection_method'] == 'column_name_hint'

    def test_emails_all_null(self):
        """Fallback to name heuristic for all-null email column."""
        df = pd.DataFrame({
            'email': [None, None, None]
        })
        issues = detect(df)
        # All null so no pattern match, but 'email' column name triggers heuristic
        assert len(issues) == 1
        assert issues[0]['sample_data']['email']['detection_method'] == 'column_name_hint'

    def test_emails_with_whitespace(self):
        """Strip whitespace before pattern matching."""
        df = pd.DataFrame({
            'email': [' alice@example.com ', ' bob@site.org\n', '  charlie@domain.com  ']
        })
        issues = detect(df)
        assert len(issues) == 1
        assert issues[0]['sample_data']['email']['match_rate'] == 100.0

    def test_email_action_labels(self):
        """Verify action labels for email PII."""
        df = pd.DataFrame({
            'email': ['alice@example.com', 'bob@site.org', 'charlie@domain.com']
        })
        issues = detect(df)
        actions = {a['id']: a for a in issues[0]['actions']}
        assert 'mask_pii' in actions
        assert 'drop_column' in actions
        assert actions['mask_pii']['label'] == 'Mask Email Address'

    def test_email_masked_samples(self):
        """Verify email samples are properly masked."""
        df = pd.DataFrame({
            'email': ['alice@example.com', 'bob@site.org']
        })
        issues = detect(df)
        samples = issues[0]['sample_data']['email']['sample_values']
        assert all('***' in s for s in samples)
        assert all('@' in s for s in samples)
        assert not any(s.startswith('alice') or s.startswith('bob') for s in samples)

    def test_email_row_indices_capped(self):
        """Cap row indices at 10 matches."""
        df = pd.DataFrame({
            'email': [f'user{i}@example.com' for i in range(20)]
        })
        issues = detect(df)
        assert len(issues[0]['row_indices']) <= 10


class TestPhoneDetection:
    """Test US phone number detection."""

    def test_phone_standard_format(self):
        """Detect standard (XXX) XXX-XXXX format."""
        df = pd.DataFrame({
            'phone': ['(555) 123-4567', '(555) 987-6543', '(415) 555-0123']
        })
        issues = detect(df)
        assert len(issues) == 1
        assert issues[0]['sample_data']['phone']['pii_type'] == 'phone'

    def test_phone_alternative_formats(self):
        """Detect alternative formats: XXX-XXX-XXXX, XXX.XXX.XXXX, +1-XXX-XXX-XXXX."""
        df = pd.DataFrame({
            'phone': ['555-123-4567', '415.555.0123', '+1-555-123-4567', '1 (555) 123-4567']
        })
        issues = detect(df)
        assert len(issues) == 1
        assert issues[0]['sample_data']['phone']['match_rate'] >= 60.0

    def test_phone_mixed_valid_invalid(self):
        """Detect phones when >60% are valid."""
        df = pd.DataFrame({
            'phone': ['555-123-4567', '415.555.0123', 'not-a-phone', '(555) 123-4567', None]
        })
        issues = detect(df)
        assert len(issues) == 1
        assert issues[0]['sample_data']['phone']['pii_type'] == 'phone'

    def test_phone_below_threshold(self):
        """Fallback to name heuristic when <60% pattern match."""
        df = pd.DataFrame({
            'phone': ['555-123-4567', 'invalid-phone', 'another-invalid', 'also-bad', '12345']
        })
        issues = detect(df)
        # Pattern match is 20%, but 'phone' column name triggers heuristic
        assert len(issues) == 1
        assert issues[0]['sample_data']['phone']['detection_method'] == 'column_name_hint'

    def test_phone_all_null(self):
        """Fallback to name heuristic for all-null phone column."""
        df = pd.DataFrame({
            'phone': [None, None, None]
        })
        issues = detect(df)
        # All null so no pattern match, but 'phone' column name triggers heuristic
        assert len(issues) == 1
        assert issues[0]['sample_data']['phone']['detection_method'] == 'column_name_hint'

    def test_phone_masked_samples(self):
        """Verify phone samples are masked (***) ***-XXXX."""
        df = pd.DataFrame({
            'phone': ['555-123-4567', '415-555-0123']
        })
        issues = detect(df)
        samples = issues[0]['sample_data']['phone']['sample_values']
        assert all('(***) ***-' in s for s in samples)
        assert all(len(s.split('-')[-1]) == 4 for s in samples)

    def test_phone_international_not_detected(self):
        """International formats outside US pattern should not match the pattern."""
        df = pd.DataFrame({
            'contact': ['+44-20-1234-5678', '+33-1-2345-6789']
        })
        issues = detect(df)
        # International formats don't match US phone pattern, neutral column name doesn't trigger heuristic
        assert len(issues) == 0

    def test_phone_column_name_heuristic(self):
        """Detect 'phone_number' column even without pattern matches."""
        df = pd.DataFrame({
            'phone_number': ['not-a-phone', 'another-invalid', 'random-text']
        })
        issues = detect(df)
        assert len(issues) == 1
        assert issues[0]['sample_data']['phone_number']['detection_method'] == 'column_name_hint'


class TestSSNDetection:
    """Test SSN (Social Security Number) detection."""

    def test_valid_ssn_format(self):
        """Detect valid XXX-XX-XXXX SSN format."""
        df = pd.DataFrame({
            'ssn': ['123-45-6789', '987-65-4321', '555-12-3456']
        })
        issues = detect(df)
        assert len(issues) == 1
        assert issues[0]['sample_data']['ssn']['pii_type'] == 'ssn'
        assert issues[0]['sample_data']['ssn']['match_rate'] == 100.0

    def test_ssn_mixed_valid_invalid(self):
        """Detect SSN when >60% are valid."""
        df = pd.DataFrame({
            'ssn': ['123-45-6789', 'invalid-ssn', '987-65-4321', 'also-invalid', '555-12-3456']
        })
        issues = detect(df)
        assert len(issues) == 1
        assert issues[0]['sample_data']['ssn']['pii_type'] == 'ssn'

    def test_ssn_below_threshold(self):
        """Fallback to name heuristic when <60% pattern match."""
        df = pd.DataFrame({
            'ssn': ['123-45-6789', 'invalid', 'also-invalid', 'not-ssn', 'random']
        })
        issues = detect(df)
        # Pattern match is 20%, but 'ssn' column name triggers heuristic
        assert len(issues) == 1
        assert issues[0]['sample_data']['ssn']['detection_method'] == 'column_name_hint'

    def test_ssn_all_null(self):
        """Fallback to name heuristic for all-null SSN column."""
        df = pd.DataFrame({
            'ssn': [None, None, None]
        })
        issues = detect(df)
        # All null so no pattern match, but 'ssn' column name triggers heuristic
        assert len(issues) == 1
        assert issues[0]['sample_data']['ssn']['detection_method'] == 'column_name_hint'

    def test_ssn_masked_samples(self):
        """Verify SSN samples are masked ***-**-XXXX."""
        df = pd.DataFrame({
            'ssn': ['123-45-6789', '987-65-4321']
        })
        issues = detect(df)
        samples = issues[0]['sample_data']['ssn']['sample_values']
        assert all('***-**-' in s for s in samples)
        assert all(len(s.split('-')[-1]) == 4 for s in samples)

    def test_social_security_column_name(self):
        """Detect 'social_security_number' column by name heuristic."""
        df = pd.DataFrame({
            'social_security_number': ['not-a-ssn', 'invalid', 'random']
        })
        issues = detect(df)
        assert len(issues) == 1
        assert issues[0]['sample_data']['social_security_number']['detection_method'] == 'column_name_hint'


class TestCreditCardDetection:
    """Test credit card number detection."""

    def test_valid_credit_card_format(self):
        """Detect valid credit card numbers XXXX-XXXX-XXXX-XXXX."""
        df = pd.DataFrame({
            'cc': ['1234-5678-9012-3456', '4111-1111-1111-1111', '5555-5555-5555-4444']
        })
        issues = detect(df)
        assert len(issues) == 1
        assert issues[0]['sample_data']['cc']['pii_type'] == 'credit_card'
        assert issues[0]['sample_data']['cc']['match_rate'] == 100.0

    def test_credit_card_no_separators(self):
        """Detect credit cards with no separators: 16 digits."""
        df = pd.DataFrame({
            'card': ['1234567890123456', '4111111111111111', '5555555555554444']
        })
        issues = detect(df)
        assert len(issues) == 1
        assert issues[0]['sample_data']['card']['pii_type'] == 'credit_card'

    def test_credit_card_mixed_valid_invalid(self):
        """Detect credit cards when >60% are valid (67% in this case)."""
        df = pd.DataFrame({
            'card': ['1234-5678-9012-3456', 'not-a-card', '4111-1111-1111-1111', '5555-5555-5555-4444', 'invalid-cc']
        })
        issues = detect(df)
        # 3 valid out of 5 = 60% match rate (exactly at threshold)
        assert len(issues) == 1
        assert issues[0]['sample_data']['card']['pii_type'] == 'credit_card'
        assert issues[0]['sample_data']['card']['match_rate'] == 60.0

    def test_credit_card_below_threshold(self):
        """Do not report if <60% match."""
        df = pd.DataFrame({
            'card': ['1234-5678-9012-3456', 'not-a-card', 'also-invalid', 'random-data']
        })
        issues = detect(df)
        assert len(issues) == 0

    def test_credit_card_masked_samples(self):
        """Verify credit card samples are masked ****-****-****-XXXX."""
        df = pd.DataFrame({
            'card': ['1234-5678-9012-3456', '4111-1111-1111-1111']
        })
        issues = detect(df)
        samples = issues[0]['sample_data']['card']['sample_values']
        assert all('****-****-****-' in s for s in samples)
        assert all(len(s.split('-')[-1]) == 4 for s in samples)

    def test_credit_card_column_name(self):
        """Detect 'credit_card' column by name heuristic."""
        df = pd.DataFrame({
            'credit_card': ['not-a-number', 'invalid', 'random']
        })
        issues = detect(df)
        assert len(issues) == 1
        assert issues[0]['sample_data']['credit_card']['detection_method'] == 'column_name_hint'


class TestNameHeuristics:
    """Test name detection via column name heuristics."""

    def test_name_column_detected(self):
        """Detect 'name' column by heuristic (low confidence)."""
        df = pd.DataFrame({
            'name': ['John Doe', 'Jane Smith', 'Bob Johnson']
        })
        issues = detect(df)
        assert len(issues) == 1
        assert issues[0]['sample_data']['name']['pii_type'] == 'name'
        assert issues[0]['sample_data']['name']['detection_method'] == 'column_name_hint'
        assert issues[0]['sample_data']['name']['match_rate'] == 0.0

    def test_firstname_lastname_detected(self):
        """Detect 'first_name' and 'last_name' columns."""
        df = pd.DataFrame({
            'first_name': ['Alice', 'Bob', 'Charlie'],
            'last_name': ['Smith', 'Jones', 'Brown']
        })
        issues = detect(df)
        assert len(issues) == 2
        issue_types = {issue['sample_data'][issue['columns'][0]]['pii_type'] for issue in issues}
        assert 'name' in issue_types

    def test_no_name_heuristic_for_actual_pattern_match(self):
        """Prioritize pattern detection over name heuristic."""
        df = pd.DataFrame({
            'name': ['alice@example.com', 'bob@site.org', 'charlie@domain.com']
        })
        issues = detect(df)
        assert len(issues) == 1
        assert issues[0]['sample_data']['name']['pii_type'] == 'email'
        assert issues[0]['sample_data']['name']['detection_method'] == 'regex_pattern'

    def test_full_name_column(self):
        """Detect 'full_name' column."""
        df = pd.DataFrame({
            'full_name': ['John Doe', 'Jane Smith', 'Bob Johnson']
        })
        issues = detect(df)
        assert len(issues) == 1
        assert issues[0]['sample_data']['full_name']['pii_type'] == 'name'

    def test_name_masked_samples(self):
        """Verify name samples are masked J*** D***."""
        df = pd.DataFrame({
            'name': ['John Doe', 'Jane Smith']
        })
        issues = detect(df)
        samples = issues[0]['sample_data']['name']['sample_values']
        assert all('***' in s for s in samples)

    def test_action_labels_for_name(self):
        """Verify action labels reference correct PII type."""
        df = pd.DataFrame({
            'full_name': ['Alice', 'Bob', 'Charlie']
        })
        issues = detect(df)
        actions = {a['id']: a for a in issues[0]['actions']}
        assert 'Mask Personal Name' in actions['mask_pii']['label']


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_empty_dataframe(self):
        """Handle empty DataFrame gracefully."""
        df = pd.DataFrame()
        issues = detect(df)
        assert issues == []

    def test_no_string_columns(self):
        """Handle DataFrame with only numeric/date columns."""
        df = pd.DataFrame({
            'count': [1, 2, 3],
            'value': [1.5, 2.5, 3.5],
        })
        issues = detect(df)
        assert len(issues) == 0

    def test_single_row_dataframe(self):
        """Handle DataFrame with single row containing PII."""
        df = pd.DataFrame({
            'data': ['alice@example.com']
        })
        issues = detect(df)
        # Single row with 100% email match rate triggers detection
        assert len(issues) == 1
        assert issues[0]['sample_data']['data']['match_rate'] == 100.0
        assert issues[0]['sample_data']['data']['pii_type'] == 'email'

    def test_column_all_nulls(self):
        """Handle column with all null values."""
        df = pd.DataFrame({
            'contact_info': [None, None, None],
            'phone': ['555-123-4567', '415-555-0123', None]
        })
        issues = detect(df)
        # 'contact_info' all null and no heuristic match, 'phone' has pattern match
        assert len(issues) == 1
        assert issues[0]['columns'] == ['phone']
        assert issues[0]['sample_data']['phone']['detection_method'] == 'regex_pattern'

    def test_column_all_nulls_multiple_columns(self):
        """Handle multiple all-null columns."""
        df = pd.DataFrame({
            'field_a': [None, None, None],
            'field_b': [None, None, None],
            'field_c': ['123-45-6789', '987-65-4321', '555-12-3456']
        })
        issues = detect(df)
        # field_a and field_b all null, no pattern, no heuristic match
        # field_c has SSN pattern match (100%)
        assert len(issues) == 1
        assert issues[0]['columns'] == ['field_c']
        assert issues[0]['sample_data']['field_c']['pii_type'] == 'ssn'

    def test_mixed_numeric_and_string_columns(self):
        """Process only string columns, ignore numeric."""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'email': ['alice@example.com', 'bob@site.org', 'charlie@domain.com'],
            'count': [10, 20, 30]
        })
        issues = detect(df)
        assert len(issues) == 1
        assert issues[0]['columns'] == ['email']

    def test_dataframe_with_dtype_str(self):
        """Handle dtype='str' (Python 3.13+ pandas 2.x behavior)."""
        df = pd.DataFrame({
            'email': pd.array(['alice@example.com', 'bob@site.org', 'charlie@domain.com'], dtype='string')
        })
        issues = detect(df)
        assert len(issues) == 1
        assert issues[0]['sample_data']['email']['match_rate'] == 100.0

    def test_dataframe_with_dtype_object(self):
        """Handle dtype='object' (traditional pandas behavior)."""
        df = pd.DataFrame({
            'email': ['alice@example.com', 'bob@site.org', 'charlie@domain.com']
        })
        issues = detect(df)
        assert len(issues) == 1
        assert issues[0]['sample_data']['email']['match_rate'] == 100.0


class TestSeverityAndActions:
    """Test severity levels and action definitions."""

    def test_pii_severity_always_high(self):
        """PII detection should always result in 'high' severity."""
        df = pd.DataFrame({
            'email': ['alice@example.com', 'bob@site.org', 'charlie@domain.com']
        })
        issues = detect(df)
        assert all(issue['severity'] == 'high' for issue in issues)

    def test_mask_action_present(self):
        """All PII issues should have a 'mask_pii' action."""
        df = pd.DataFrame({
            'email': ['alice@example.com', 'bob@site.org', 'charlie@domain.com'],
            'ssn': ['123-45-6789', '987-65-4321', '555-12-3456']
        })
        issues = detect(df)
        for issue in issues:
            action_ids = [a['id'] for a in issue['actions']]
            assert 'mask_pii' in action_ids

    def test_drop_column_action_present(self):
        """All PII issues should have a 'drop_column' action."""
        df = pd.DataFrame({
            'email': ['alice@example.com', 'bob@site.org', 'charlie@domain.com']
        })
        issues = detect(df)
        action_ids = [a['id'] for a in issues[0]['actions']]
        assert 'drop_column' in action_ids

    def test_action_parameters(self):
        """Verify action parameters are correct."""
        df = pd.DataFrame({
            'email': ['alice@example.com', 'bob@site.org']
        })
        issues = detect(df)
        mask_action = next(a for a in issues[0]['actions'] if a['id'] == 'mask_pii')
        assert mask_action['params']['column'] == 'email'
        assert mask_action['params']['pii_type'] == 'email'
        assert mask_action['params']['mask_type'] == 'partial'

    def test_action_descriptions_reference_pii_type(self):
        """Action descriptions should reference the detected PII type."""
        df = pd.DataFrame({
            'phone': ['555-123-4567', '415-555-0123', '(555) 123-4567']
        })
        issues = detect(df)
        actions = {a['id']: a for a in issues[0]['actions']}
        assert 'Phone Number' in actions['mask_pii']['description']

    def test_issue_schema_completeness(self):
        """Verify all required schema fields are present."""
        df = pd.DataFrame({
            'email': ['alice@example.com', 'bob@site.org']
        })
        issues = detect(df)
        required_fields = ['detector', 'type', 'columns', 'severity', 'row_indices', 'summary', 'sample_data', 'actions']
        for issue in issues:
            for field in required_fields:
                assert field in issue, f"Missing field: {field}"

    def test_sample_data_structure(self):
        """Verify sample_data has correct structure."""
        df = pd.DataFrame({
            'email': ['alice@example.com', 'bob@site.org']
        })
        issues = detect(df)
        col = 'email'
        sample_data = issues[0]['sample_data'][col]
        assert 'pii_type' in sample_data
        assert 'match_rate' in sample_data
        assert 'sample_values' in sample_data
        assert 'detection_method' in sample_data


class TestIntegration:
    """Test integration scenarios with multiple PII types."""

    def test_multiple_pii_columns(self):
        """Detect multiple PII columns in one DataFrame."""
        df = pd.DataFrame({
            'email': ['alice@example.com', 'bob@site.org', 'charlie@domain.com'],
            'phone': ['555-123-4567', '415-555-0123', '(555) 123-4567'],
            'ssn': ['123-45-6789', '987-65-4321', '555-12-3456']
        })
        issues = detect(df)
        assert len(issues) == 3
        columns = {issue['columns'][0] for issue in issues}
        assert columns == {'email', 'phone', 'ssn'}

    def test_pattern_priority_order(self):
        """Test that SSN (highest priority) is detected over other patterns."""
        df = pd.DataFrame({
            'mixed': ['123-45-6789', '987-65-4321', '555-12-3456', 'invalid']
        })
        issues = detect(df)
        # 3 SSNs + 1 invalid = 75% match rate, should detect as SSN not other types
        assert len(issues) == 1
        assert issues[0]['sample_data']['mixed']['pii_type'] == 'ssn'

    def test_no_duplicate_issues(self):
        """Ensure no duplicate issues for same column."""
        df = pd.DataFrame({
            'user_contact': ['alice@example.com', 'bob@site.org', 'charlie@domain.com']
        })
        issues = detect(df)
        assert len(issues) == 1

    def test_pattern_match_rate_accuracy(self):
        """Verify match_rate is calculated correctly."""
        df = pd.DataFrame({
            'email': ['alice@example.com', 'bob@site.org', 'invalid-email', None]
        })
        issues = detect(df)
        non_null_count = 3
        match_count = 2
        expected_rate = (match_count / non_null_count) * 100
        actual_rate = issues[0]['sample_data']['email']['match_rate']
        assert abs(actual_rate - expected_rate) < 0.1

    def test_row_indices_valid_integers(self):
        """Verify row indices are valid integers."""
        df = pd.DataFrame({
            'email': ['alice@example.com', 'bob@site.org', 'charlie@domain.com']
        })
        issues = detect(df)
        row_indices = issues[0]['row_indices']
        assert all(isinstance(i, int) for i in row_indices)
        assert all(0 <= i < len(df) for i in row_indices)

    def test_sample_values_not_empty_for_pattern_matches(self):
        """Pattern-based detections should have sample values."""
        df = pd.DataFrame({
            'email': ['alice@example.com', 'bob@site.org', 'charlie@domain.com']
        })
        issues = detect(df)
        samples = issues[0]['sample_data']['email']['sample_values']
        assert len(samples) > 0

    def test_sample_values_can_be_empty_for_name_heuristic(self):
        """Name heuristic detections may have empty sample values."""
        df = pd.DataFrame({
            'name': ['not-an-email', 'invalid', 'random']
        })
        issues = detect(df)
        samples = issues[0]['sample_data']['name']['sample_values']
        assert isinstance(samples, list)
