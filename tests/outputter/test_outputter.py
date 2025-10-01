"""Tests for the main Outputter factory class."""

import unittest
from unittest.mock import Mock, patch

from focus_validator.outputter.outputter import Outputter
from focus_validator.exceptions import FocusNotImplementedError
from focus_validator.rules.spec_rules import ValidationResults


class TestOutputter(unittest.TestCase):
    """Test the main Outputter factory class."""

    def test_console_outputter_creation(self):
        """Test that console outputter is created correctly."""
        outputter = Outputter(output_type="console", output_destination=None)
        self.assertEqual(outputter.outputter.__class__.__name__, "ConsoleOutputter")

    def test_unittest_outputter_creation(self):
        """Test that unittest outputter is created correctly."""
        outputter = Outputter(output_type="unittest", output_destination="test.xml")
        self.assertEqual(outputter.outputter.__class__.__name__, "UnittestOutputter")

    def test_unsupported_output_type_raises_error(self):
        """Test that unsupported output type raises FocusNotImplementedError."""
        with self.assertRaises(FocusNotImplementedError) as cm:
            Outputter(output_type="unsupported", output_destination=None)
        self.assertEqual(str(cm.exception), "Output type not supported")

    @patch('focus_validator.outputter.outputter_console.ConsoleOutputter.write')
    def test_write_delegates_to_console_outputter(self, mock_write):
        """Test that write method delegates to the underlying console outputter."""
        outputter = Outputter(output_type="console", output_destination=None)
        
        # Create mock ValidationResults
        mock_results = Mock(spec=ValidationResults)
        
        outputter.write(mock_results)
        mock_write.assert_called_once_with(mock_results)

    @patch('focus_validator.outputter.outputter_unittest.UnittestOutputter.write')
    def test_write_delegates_to_unittest_outputter(self, mock_write):
        """Test that write method delegates to the underlying unittest outputter."""
        outputter = Outputter(output_type="unittest", output_destination="test.xml")
        
        # Create mock ValidationResults
        mock_results = Mock(spec=ValidationResults)
        
        outputter.write(mock_results)
        mock_write.assert_called_once_with(mock_results)

    def test_outputter_stores_destination_correctly(self):
        """Test that output destination is passed to underlying outputters."""
        console_outputter = Outputter(output_type="console", output_destination="console_dest")
        unittest_outputter = Outputter(output_type="unittest", output_destination="unittest_dest")
        
        self.assertEqual(console_outputter.outputter.output_destination, "console_dest")
        self.assertEqual(unittest_outputter.outputter.output_destination, "unittest_dest")

    def test_outputter_has_logger(self):
        """Test that outputter has a logger configured."""
        outputter = Outputter(output_type="console", output_destination=None)
        self.assertIsNotNone(outputter.log)
        self.assertEqual(outputter.log.name, "focus_validator.outputter.outputter.Outputter")


if __name__ == '__main__':
    unittest.main()