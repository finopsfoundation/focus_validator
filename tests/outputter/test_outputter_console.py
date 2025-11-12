"""Tests for the ConsoleOutputter class."""

import unittest
from unittest.mock import Mock, patch
from io import StringIO
import sys

from focus_validator.outputter.outputter_console import (
    ConsoleOutputter, 
    _status_from_result, 
    _line_for_rule,
    STATUS_PASS,
    STATUS_FAIL, 
    STATUS_SKIP
)
from focus_validator.rules.spec_rules import ValidationResults
from focus_validator.config_objects.rule import ModelRule


class TestConsoleOutputterHelpers(unittest.TestCase):
    """Test helper functions for console output formatting."""

    def test_status_from_result_pass(self):
        """Test status detection for passing results."""
        entry = {"ok": True, "details": {}}
        self.assertEqual(_status_from_result(entry), STATUS_PASS)

    def test_status_from_result_fail(self):
        """Test status detection for failing results."""
        entry = {"ok": False, "details": {}}
        self.assertEqual(_status_from_result(entry), STATUS_FAIL)

    def test_status_from_result_skipped(self):
        """Test status detection for skipped results."""
        entry = {"ok": True, "details": {"skipped": True}}
        self.assertEqual(_status_from_result(entry), STATUS_SKIP)

    def test_status_from_result_missing_details(self):
        """Test status detection with missing details."""
        entry = {"ok": True}
        self.assertEqual(_status_from_result(entry), STATUS_PASS)
        
        entry = {"ok": False}
        self.assertEqual(_status_from_result(entry), STATUS_FAIL)

    def test_line_for_rule_pass(self):
        """Test line formatting for passing rule."""
        rule_id = "TestRule-001-M"
        entry = {
            "ok": True, 
            "details": {
                "violations": 0, 
                "timing_ms": 1.5,
                "message": "All good"
            }
        }
        line = _line_for_rule(rule_id, entry)
        self.assertIn("✅", line)
        self.assertIn("TestRule-001-M", line)
        self.assertIn("PASS", line)
        self.assertIn("violations=0", line)
        self.assertIn("1.5ms", line)
        self.assertIn("msg=All good", line)

    def test_line_for_rule_fail(self):
        """Test line formatting for failing rule."""
        rule_id = "TestRule-002-M"
        entry = {
            "ok": False, 
            "details": {
                "violations": 5, 
                "reason": "Data validation failed"
            }
        }
        line = _line_for_rule(rule_id, entry)
        self.assertIn("❌", line)
        self.assertIn("TestRule-002-M", line)
        self.assertIn("FAIL", line)
        self.assertIn("violations=5", line)
        self.assertIn("reason=Data validation failed", line)

    def test_line_for_rule_skipped(self):
        """Test line formatting for skipped rule."""
        rule_id = "TestRule-003-O"
        entry = {
            "ok": True, 
            "details": {
                "skipped": True,
                "reason": "dynamic rule",
                "message": "dynamic rule"
            }
        }
        line = _line_for_rule(rule_id, entry)
        self.assertIn("⏭️", line)
        self.assertIn("TestRule-003-O", line)
        self.assertIn("SKIPPED", line)
        self.assertIn("reason=dynamic rule", line)
        self.assertIn("msg=dynamic rule", line)

    def test_line_for_rule_minimal_details(self):
        """Test line formatting with minimal details."""
        rule_id = "MinimalRule"
        entry = {"ok": True, "details": {}}
        line = _line_for_rule(rule_id, entry)
        self.assertIn("✅", line)
        self.assertIn("MinimalRule", line)
        self.assertIn("PASS", line)
        # Should not have extra parentheses when no details
        self.assertNotIn("()", line)


class TestConsoleOutputter(unittest.TestCase):
    """Test the ConsoleOutputter class."""

    def setUp(self):
        """Set up test fixtures."""
        self.outputter = ConsoleOutputter(output_destination=None)
        
        # Mock rule objects
        self.mock_rule = Mock(spec=ModelRule)
        # Create a mock validation_criteria with must_satisfy
        mock_validation_criteria = Mock()
        mock_validation_criteria.must_satisfy = "Test requirement"
        self.mock_rule.validation_criteria = mock_validation_criteria
        
        # Sample ValidationResults
        self.sample_results = ValidationResults(
            by_idx={
                0: {"ok": True, "details": {"violations": 0, "timing_ms": 1.0}, "rule_id": "Pass-001-M"},
                1: {"ok": False, "details": {"violations": 2, "message": "Failed check"}, "rule_id": "Fail-002-M"},
                2: {"ok": True, "details": {"skipped": True, "reason": "dynamic rule"}, "rule_id": "Skip-003-O"}
            },
            by_rule_id={
                "Pass-001-M": {"ok": True, "details": {"violations": 0, "timing_ms": 1.0}, "rule_id": "Pass-001-M"},
                "Fail-002-M": {"ok": False, "details": {"violations": 2, "message": "Failed check"}, "rule_id": "Fail-002-M"},
                "Skip-003-O": {"ok": True, "details": {"skipped": True, "reason": "dynamic rule"}, "rule_id": "Skip-003-O"}
            },
            rules={
                "Pass-001-M": self.mock_rule,
                "Fail-002-M": self.mock_rule,
                "Skip-003-O": self.mock_rule
            },
            rules_version="test_rules_version",
            data_filename="test_data.csv", 
            data_row_count=100,
            model_version="test_model_version",
            focus_dataset="CostAndUsage"
        )

    def test_outputter_initialization(self):
        """Test ConsoleOutputter initialization."""
        outputter = ConsoleOutputter("test_destination")
        self.assertEqual(outputter.output_destination, "test_destination")
        self.assertIsNone(outputter.result_set)
        self.assertIsNotNone(outputter.log)

    def test_write_rejects_old_api(self):
        """Test that write method rejects objects without by_rule_id."""
        old_style_results = Mock()
        del old_style_results.by_rule_id  # Ensure it doesn't have the attribute
        
        with self.assertRaises(TypeError) as cm:
            self.outputter.write(old_style_results)
        
        self.assertIn("expected ValidationResults with by_rule_id", str(cm.exception))

    @patch('builtins.print')
    def test_write_output_format(self, mock_print):
        """Test the complete output format."""
        self.outputter.write(self.sample_results)
        
        # Verify print calls
        calls = [call[0][0] for call in mock_print.call_args_list]
        
        # Check for header
        self.assertTrue(any("=== Validation Results ===" in call for call in calls))
        
        # Check for summary line
        summary_calls = [call for call in calls if "Total:" in call and "Pass:" in call]
        self.assertEqual(len(summary_calls), 1)
        summary = summary_calls[0]
        self.assertIn("Total: 3", summary)
        self.assertIn("Pass: 1", summary)
        self.assertIn("Fail: 1", summary)
        self.assertIn("Skipped: 1", summary)
        
        # Check for individual rule lines
        rule_lines = [call for call in calls if any(rule_id in call for rule_id in ["Pass-001-M", "Fail-002-M", "Skip-003-O"])]
        self.assertGreaterEqual(len(rule_lines), 3)  # Should have at least 3 rule lines (may include failure details)
        
        # Check for failures section
        self.assertTrue(any("--- Failures ---" in call for call in calls))

    @patch('builtins.print')
    def test_write_no_failures_no_failure_section(self, mock_print):
        """Test that no failure section is shown when all tests pass."""
        all_pass_results = ValidationResults(
            by_idx={
                0: {"ok": True, "details": {"violations": 0}, "rule_id": "Pass-001-M"}
            },
            by_rule_id={
                "Pass-001-M": {"ok": True, "details": {"violations": 0}, "rule_id": "Pass-001-M"}
            },
            rules={"Pass-001-M": self.mock_rule},
            rules_version="test_rules_version",
            data_filename="test_data.csv", 
            data_row_count=100,
            model_version="test_model_version",
            focus_dataset="CostAndUsage"
        )
        
        self.outputter.write(all_pass_results)
        
        # Verify no failure section
        calls = [call[0][0] for call in mock_print.call_args_list]
        self.assertFalse(any("--- Failures ---" in call for call in calls))

    @patch('builtins.print')
    def test_write_failure_details_include_must_satisfy(self, mock_print):
        """Test that failure details include MustSatisfy information."""
        self.outputter.write(self.sample_results)
        
        calls = [call[0][0] for call in mock_print.call_args_list]
        
        # Find failure detail lines
        failure_detail_calls = [call for call in calls if "Fail-002-M:" in call or "MustSatisfy:" in call]
        self.assertTrue(len(failure_detail_calls) >= 2)  # At least the failure line and MustSatisfy line
        
        # Check MustSatisfy is included
        must_satisfy_calls = [call for call in calls if "MustSatisfy: Test requirement" in call]
        self.assertEqual(len(must_satisfy_calls), 1)

    @patch('builtins.print')
    def test_write_handles_missing_rule_gracefully(self, mock_print):
        """Test handling of missing rule objects gracefully."""
        results_missing_rule = ValidationResults(
            by_idx={
                0: {"ok": False, "details": {"violations": 1}, "rule_id": "Missing-Rule"}
            },
            by_rule_id={
                "Missing-Rule": {"ok": False, "details": {"violations": 1}, "rule_id": "Missing-Rule"}
            },
            rules={},  # No rules provided
            rules_version="test_rules_version",
            data_filename="test_data.csv", 
            data_row_count=100,
            model_version="test_model_version",
            focus_dataset="CostAndUsage"
        )
        
        self.outputter.write(results_missing_rule)
        
        calls = [call[0][0] for call in mock_print.call_args_list]
        must_satisfy_calls = [call for call in calls if "MustSatisfy: N/A" in call]
        self.assertEqual(len(must_satisfy_calls), 1)

    @patch('builtins.print')
    def test_line_ordering_is_stable(self, mock_print):
        """Test that output lines are sorted for stable ordering."""
        # Create results with rule IDs that would be in different order
        unordered_results = ValidationResults(
            by_idx={
                0: {"ok": True, "details": {}, "rule_id": "Zebra-001-M"},
                1: {"ok": True, "details": {}, "rule_id": "Alpha-002-M"},
                2: {"ok": True, "details": {}, "rule_id": "Beta-003-M"}
            },
            by_rule_id={
                "Zebra-001-M": {"ok": True, "details": {}, "rule_id": "Zebra-001-M"},
                "Alpha-002-M": {"ok": True, "details": {}, "rule_id": "Alpha-002-M"},
                "Beta-003-M": {"ok": True, "details": {}, "rule_id": "Beta-003-M"}
            },
            rules={},
            rules_version="test_rules_version",
            data_filename="test_data.csv", 
            data_row_count=100,
            model_version="test_model_version",
            focus_dataset="CostAndUsage"
        )
        
        self.outputter.write(unordered_results)
        
        calls = [call[0][0] for call in mock_print.call_args_list]
        rule_lines = [call for call in calls if any(rule_id in call for rule_id in ["Zebra-001-M", "Alpha-002-M", "Beta-003-M"])]
        
        # Extract rule IDs from lines to check order
        rule_ids_in_order = []
        for line in rule_lines:
            if "Alpha-002-M" in line:
                rule_ids_in_order.append("Alpha")
            elif "Beta-003-M" in line:
                rule_ids_in_order.append("Beta")
            elif "Zebra-001-M" in line:
                rule_ids_in_order.append("Zebra")
        
        # Should be alphabetically sorted
        self.assertEqual(rule_ids_in_order, ["Alpha", "Beta", "Zebra"])

    def test_outputter_logger_name(self):
        """Test that outputter has correct logger name."""
        self.assertEqual(
            self.outputter.log.name, 
            "focus_validator.outputter.outputter_console.ConsoleOutputter"
        )


if __name__ == '__main__':
    unittest.main()