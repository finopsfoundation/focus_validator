"""Tests for the UnittestOutputter and UnittestFormatter classes."""

import unittest
from unittest.mock import Mock, patch, mock_open
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
import tempfile
import os

from focus_validator.outputter.outputter_unittest import UnittestOutputter, UnittestFormatter
from focus_validator.config_objects.rule import ChecklistObject, ChecklistObjectStatus, ModelRule, InvalidRule


class TestUnittestFormatter(unittest.TestCase):
    """Test the UnittestFormatter class."""

    def setUp(self):
        """Set up test fixtures."""
        self.formatter = UnittestFormatter(
            name="Test Suite",
            tests=10,
            failures=2,
            errors=1,
            skipped=3
        )

    def test_formatter_initialization_with_defaults(self):
        """Test UnittestFormatter initialization with default values."""
        formatter = UnittestFormatter(
            name="Default Test",
            tests=5,
            failures=1,
            errors=0,
            skipped=2
        )
        
        self.assertEqual(formatter.name, "Default Test")
        self.assertEqual(formatter.tests, "5")
        self.assertEqual(formatter.failures, "1")
        self.assertEqual(formatter.errors, "0")
        self.assertEqual(formatter.skipped, "2")
        self.assertEqual(formatter.assertions, "None")  # str(None) = 'None', and 'None' is truthy so doesn't trigger fallback
        self.assertIsNotNone(formatter.timestamp)
        self.assertEqual(formatter.time, "0")

    def test_formatter_initialization_with_custom_values(self):
        """Test UnittestFormatter initialization with custom values."""
        custom_time = "123.45"
        custom_timestamp = "2023-10-01T10:00:00"
        
        formatter = UnittestFormatter(
            name="Custom Test",
            tests=8,
            failures=1,
            errors=0,
            skipped=1,
            assertions=10,
            time=custom_time,
            timestamp=custom_timestamp
        )
        
        self.assertEqual(formatter.assertions, "10")
        self.assertEqual(formatter.time, custom_time)
        self.assertEqual(formatter.timestamp, custom_timestamp)

    def test_add_testsuite(self):
        """Test adding a test suite."""
        self.formatter.add_testsuite("Suite1", "Column1")
        
        self.assertIn("Suite1", self.formatter.results)
        self.assertEqual(self.formatter.results["Suite1"]["column"], "Column1")
        self.assertEqual(self.formatter.results["Suite1"]["time"], "0")
        self.assertEqual(self.formatter.results["Suite1"]["tests"], {})

    def test_add_testcase(self):
        """Test adding a test case to a suite."""
        self.formatter.add_testsuite("Suite1", "Column1")
        self.formatter.add_testcase(
            testsuite="Suite1",
            name="Test1",
            result="passed",
            message="Test passed successfully",
            check_type_name="Format"
        )
        
        test_case = self.formatter.results["Suite1"]["tests"]["Test1"]
        self.assertEqual(test_case["result"], "passed")
        self.assertEqual(test_case["message"], "Test passed successfully")
        self.assertEqual(test_case["check_type_name"], "Format")

    def test_generate_unittest_basic_structure(self):
        """Test basic XML structure generation."""
        self.formatter.add_testsuite("TestSuite", "TestColumn")
        self.formatter.add_testcase("TestSuite", "Test1", "passed", "Success", "Format")
        
        tree = self.formatter.generate_unittest()
        root = tree.getroot()
        
        # Verify root element
        self.assertEqual(root.tag, "testsuites")
        self.assertEqual(root.get("name"), "Test Suite")
        self.assertEqual(root.get("tests"), "10")
        self.assertEqual(root.get("failures"), "2")
        self.assertEqual(root.get("errors"), "1")
        self.assertEqual(root.get("skipped"), "3")

    def test_generate_unittest_with_failure(self):
        """Test XML generation with failure cases."""
        self.formatter.add_testsuite("FailSuite", "FailColumn")
        self.formatter.add_testcase("FailSuite", "FailTest", "failed", "Test failed", "Validation")
        
        tree = self.formatter.generate_unittest()
        root = tree.getroot()
        
        # Find the failure element
        failure_elements = root.findall(".//failure")
        self.assertEqual(len(failure_elements), 1)
        
        failure = failure_elements[0]
        self.assertEqual(failure.get("name"), "FailTest")
        self.assertEqual(failure.get("message"), "Test failed")
        self.assertEqual(failure.get("type"), "AssertionError")
        self.assertEqual(failure.text, "Failed")

    def test_generate_unittest_with_skipped(self):
        """Test XML generation with skipped cases."""
        self.formatter.add_testsuite("SkipSuite", "SkipColumn")
        self.formatter.add_testcase("SkipSuite", "SkipTest", "skipped", "Test skipped", "Optional")
        
        tree = self.formatter.generate_unittest()
        root = tree.getroot()
        
        # Find the skipped element
        skipped_elements = root.findall(".//skipped")
        self.assertEqual(len(skipped_elements), 1)
        
        skipped = skipped_elements[0]
        self.assertEqual(skipped.get("message"), "Test skipped")

    def test_generate_unittest_with_error(self):
        """Test XML generation with error cases."""
        self.formatter.add_testsuite("ErrorSuite", "ErrorColumn")
        self.formatter.add_testcase("ErrorSuite", "ErrorTest", "errored", "Test errored", "Critical")
        
        tree = self.formatter.generate_unittest()
        root = tree.getroot()
        
        # Find the error element
        error_elements = root.findall(".//error")
        self.assertEqual(len(error_elements), 1)
        
        error = error_elements[0]
        self.assertEqual(error.get("message"), "Test errored")

    def test_generate_unittest_sorts_suites_and_cases(self):
        """Test that test suites and cases are sorted in output."""
        # Add suites in non-alphabetical order
        self.formatter.add_testsuite("ZSuite", "ZColumn")
        self.formatter.add_testsuite("ASuite", "AColumn")
        self.formatter.add_testsuite("MSuite", "MColumn")
        
        # Add test cases in non-alphabetical order
        self.formatter.add_testcase("ASuite", "ZTest", "passed", "Z Test", "Format")
        self.formatter.add_testcase("ASuite", "ATest", "passed", "A Test", "Format")
        
        tree = self.formatter.generate_unittest()
        root = tree.getroot()
        
        # Verify suite order (should be sorted)
        testsuites = root.findall("testsuite")
        suite_names = [ts.get("name") for ts in testsuites]
        expected_names = ["ASuite-AColumn", "MSuite-MColumn", "ZSuite-ZColumn"]
        self.assertEqual(suite_names, expected_names)
        
        # Verify test case order within suite (should be sorted)
        asuite = testsuites[0]  # First suite is ASuite
        testcases = asuite.findall("testcase")
        case_names = [tc.get("name") for tc in testcases]
        self.assertTrue(case_names[0].startswith("ATest"))
        self.assertTrue(case_names[1].startswith("ZTest"))

    def test_testcase_name_format(self):
        """Test that test case names include check type information."""
        self.formatter.add_testsuite("NameSuite", "NameColumn")
        self.formatter.add_testcase("NameSuite", "TestCase1", "passed", "Message", "FormatCheck")
        
        tree = self.formatter.generate_unittest()
        testcase = tree.find(".//testcase")
        
        expected_name = "TestCase1 :: FormatCheck"
        self.assertEqual(testcase.get("name"), expected_name)

    @patch('focus_validator.outputter.outputter_unittest.sys.version_info', (3, 8))
    def test_generate_unittest_python38_compatibility(self):
        """Test that Python 3.8 compatibility warning is logged."""
        with patch.object(self.formatter, 'log') as mock_log:
            tree = self.formatter.generate_unittest()
            mock_log.warning.assert_called_once()
            self.assertIn("produced output not indent", mock_log.warning.call_args[0][0])


class TestUnittestOutputter(unittest.TestCase):
    """Test the UnittestOutputter class."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.xml')
        self.temp_file.close()
        self.output_file = self.temp_file.name
        self.outputter = UnittestOutputter(self.output_file)

    def tearDown(self):
        """Clean up test fixtures."""
        if os.path.exists(self.output_file):
            os.unlink(self.output_file)

    def create_mock_checklist_object(self, check_name, status, column_id="TestColumn", error=None):
        """Helper to create mock ChecklistObject."""
        mock_rule_ref = Mock(spec=ModelRule)
        mock_rule_ref.check_type_friendly_name = "TestCheckType"
        
        obj = Mock(spec=ChecklistObject)
        obj.check_name = check_name
        obj.status = ChecklistObjectStatus(status)
        obj.column_id = column_id
        obj.friendly_name = f"Friendly name for {check_name}"
        obj.error = error
        obj.rule_ref = mock_rule_ref
        
        # Mock model_dump method
        obj.model_dump.return_value = {
            "check_name": check_name,
            "status": ChecklistObjectStatus(status),
            "column_id": column_id,
            "friendly_name": f"Friendly name for {check_name}",
            "error": error,
            "rule_ref": {"check_type_friendly_name": "TestCheckType"}
        }
        
        return obj

    def test_outputter_initialization(self):
        """Test UnittestOutputter initialization."""
        outputter = UnittestOutputter("test_output.xml")
        self.assertEqual(outputter.output_destination, "test_output.xml")

    def test_write_creates_xml_file(self):
        """Test that write method creates an XML file."""
        # Create mock result set
        mock_checklist_obj = self.create_mock_checklist_object("Test-001-M", "passed")
        
        mock_result_set = Mock()
        mock_result_set.checklist = {"Test-001-M": mock_checklist_obj}
        
        self.outputter.write(mock_result_set)
        
        # Verify file was created and is valid XML
        self.assertTrue(os.path.exists(self.output_file))
        
        tree = ET.parse(self.output_file)
        root = tree.getroot()
        self.assertEqual(root.tag, "testsuites")

    def test_write_handles_multiple_statuses(self):
        """Test writing results with multiple test statuses."""
        # Create objects with different statuses
        passed_obj = self.create_mock_checklist_object("Pass-001-M", "passed")
        failed_obj = self.create_mock_checklist_object("Fail-002-M", "failed")
        skipped_obj = self.create_mock_checklist_object("Skip-003-O", "skipped")
        errored_obj = self.create_mock_checklist_object("Error-004-C", "errored", error="System error")
        
        mock_result_set = Mock()
        mock_result_set.checklist = {
            "Pass-001-M": passed_obj,
            "Fail-002-M": failed_obj,
            "Skip-003-O": skipped_obj,
            "Error-004-C": errored_obj
        }
        
        self.outputter.write(mock_result_set)
        
        # Parse and verify XML structure
        tree = ET.parse(self.output_file)
        root = tree.getroot()
        
        # Check summary counts
        self.assertEqual(root.get("tests"), "4")
        self.assertEqual(root.get("failures"), "1")
        self.assertEqual(root.get("errors"), "1")
        self.assertEqual(root.get("skipped"), "1")
        
        # Verify different test result types exist
        self.assertEqual(len(root.findall(".//failure")), 1)
        self.assertEqual(len(root.findall(".//error")), 1)
        self.assertEqual(len(root.findall(".//skipped")), 1)

    def test_write_groups_by_test_suite(self):
        """Test that tests are grouped by test suite (rule prefix)."""
        obj1 = self.create_mock_checklist_object("GroupA-001-M", "passed", "ColumnA")
        obj2 = self.create_mock_checklist_object("GroupA-002-M", "failed", "ColumnA")
        obj3 = self.create_mock_checklist_object("GroupB-001-M", "passed", "ColumnB")
        
        mock_result_set = Mock()
        mock_result_set.checklist = {
            "GroupA-001-M": obj1,
            "GroupA-002-M": obj2,
            "GroupB-001-M": obj3
        }
        
        self.outputter.write(mock_result_set)
        
        # Parse and verify grouping
        tree = ET.parse(self.output_file)
        root = tree.getroot()
        
        testsuites = root.findall("testsuite")
        # Should have GroupA and GroupB suites, plus potentially Base suite
        suite_names = {ts.get("name") for ts in testsuites}
        
        # Check that GroupA and GroupB suites exist
        self.assertTrue(any("GroupA" in name for name in suite_names))
        self.assertTrue(any("GroupB" in name for name in suite_names))

    def test_write_handles_errored_cases_in_base_suite(self):
        """Test that errored cases are handled in Base suite."""
        errored_obj = self.create_mock_checklist_object("Error-001-M", "errored", error="Critical error")
        
        mock_result_set = Mock()
        mock_result_set.checklist = {"Error-001-M": errored_obj}
        
        self.outputter.write(mock_result_set)
        
        # Parse and verify Base suite creation for errors
        tree = ET.parse(self.output_file)
        root = tree.getroot()
        
        # Should have Base suite for errors
        base_suite = None
        for testsuite in root.findall("testsuite"):
            if "Base" in testsuite.get("name", ""):
                base_suite = testsuite
                break
        
        self.assertIsNotNone(base_suite, "Base suite should be created for errored cases")
        
        # Verify error testcase exists in Base suite
        error_testcases = base_suite.findall("testcase")
        self.assertEqual(len(error_testcases), 1)
        
        error_elements = base_suite.findall(".//error")
        self.assertEqual(len(error_elements), 1)
        self.assertEqual(error_elements[0].get("message"), "Critical error")

    def test_xml_declaration_and_encoding(self):
        """Test that XML file has proper declaration and UTF-8 encoding."""
        mock_checklist_obj = self.create_mock_checklist_object("Test-001-M", "passed")
        
        mock_result_set = Mock()
        mock_result_set.checklist = {"Test-001-M": mock_checklist_obj}
        
        self.outputter.write(mock_result_set)
        
        # Read raw file content to check XML declaration
        with open(self.output_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        self.assertTrue(content.startswith('<?xml version=\'1.0\' encoding=\'utf-8\'?>'))

    def test_testsuite_naming_includes_column(self):
        """Test that test suite names include column information."""
        obj = self.create_mock_checklist_object("Suite-001-M", "passed", "TestColumn")
        
        mock_result_set = Mock()
        mock_result_set.checklist = {"Suite-001-M": obj}
        
        self.outputter.write(mock_result_set)
        
        # Parse and verify suite naming
        tree = ET.parse(self.output_file)
        testsuite = tree.find("testsuite")
        
        suite_name = testsuite.get("name")
        self.assertIn("Suite", suite_name)
        self.assertIn("TestColumn", suite_name)

    def test_empty_checklist_creates_valid_xml(self):
        """Test that empty checklist still creates valid XML."""
        mock_result_set = Mock()
        mock_result_set.checklist = {}
        
        self.outputter.write(mock_result_set)
        
        # Verify file exists and is valid XML
        self.assertTrue(os.path.exists(self.output_file))
        
        tree = ET.parse(self.output_file)
        root = tree.getroot()
        self.assertEqual(root.tag, "testsuites")
        self.assertEqual(root.get("tests"), "0")
        self.assertEqual(root.get("failures"), "0")
        self.assertEqual(root.get("errors"), "0")
        self.assertEqual(root.get("skipped"), "0")


if __name__ == '__main__':
    unittest.main()