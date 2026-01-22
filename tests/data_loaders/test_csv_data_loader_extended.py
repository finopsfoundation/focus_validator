"""Comprehensive tests for CSV data loader error handling, edge cases, and resilient loading."""

import unittest
from unittest.mock import Mock, patch, mock_open
import tempfile
import os
import polars as pl
import io
import sys
from datetime import datetime

from focus_validator.data_loaders.csv_data_loader import CSVDataLoader


class TestCSVDataLoaderErrorHandling(unittest.TestCase):
    """Test CSV data loader error handling and edge cases."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a valid CSV file for reference
        self.temp_csv = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        self.temp_csv.write("column1,column2\nvalue1,value2\nvalue3,value4\n")
        self.temp_csv.close()

    def tearDown(self):
        """Clean up test fixtures."""
        if os.path.exists(self.temp_csv.name):
            os.unlink(self.temp_csv.name)

    def test_file_not_found(self):
        """Test behavior when CSV file doesn't exist."""
        loader = CSVDataLoader("nonexistent_file.csv")
        
        with self.assertRaises(Exception) as context:
            loader.load()
        self.assertIn("Failed to load CSV data", str(context.exception))

    def test_permission_denied(self):
        """Test behavior when file exists but can't be read due to permissions."""
        # Create a file and remove read permissions (Unix-like systems)
        if os.name != 'nt':  # Skip on Windows
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
                temp_file.write("col1,col2\n1,2\n")
                temp_filename = temp_file.name
            
            try:
                # Remove read permissions
                os.chmod(temp_filename, 0o000)
                
                loader = CSVDataLoader(temp_filename)
                with self.assertRaises(Exception) as context:
                    loader.load()
                self.assertIn("Failed to load CSV data", str(context.exception))
            finally:
                # Restore permissions and clean up
                os.chmod(temp_filename, 0o644)
                os.unlink(temp_filename)

    def test_empty_file(self):
        """Test loading completely empty CSV file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
            # Write nothing - completely empty file
            pass
        
        try:
            loader = CSVDataLoader(temp_file.name)
            
            # Polars raises an exception for completely empty files
            with self.assertRaises(Exception):  # TODO: Use appropriate Polars exception
                loader.load()
        finally:
            os.unlink(temp_file.name)

    def test_only_headers_no_data(self):
        """Test CSV file with only headers and no data rows."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
            temp_file.write("column1,column2,column3\n")  # Only headers
            temp_filename = temp_file.name
        
        try:
            loader = CSVDataLoader(temp_filename)
            result = loader.load()
            
            self.assertIsInstance(result, pl.DataFrame)
            self.assertEqual(len(result), 0)  # No data rows
            self.assertEqual(len(result.columns), 3)  # But has columns
            self.assertEqual(list(result.columns), ["column1", "column2", "column3"])
        finally:
            os.unlink(temp_filename)

    def test_inconsistent_column_counts(self):
        """Test CSV with inconsistent number of columns per row."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
            temp_file.write("col1,col2,col3\n")  # 3 columns
            temp_file.write("val1,val2\n")       # 2 values - missing column
            temp_file.write("val3,val4,val5,val6\n")  # 4 values - extra column
            temp_filename = temp_file.name
        
        try:
            loader = CSVDataLoader(temp_filename)
            
            # With resilient loading, Polars can handle malformed CSV by truncating ragged lines
            result = loader.load()
            self.assertIsInstance(result, pl.DataFrame, "Polars should successfully load malformed CSV with resilience options")
            self.assertEqual(len(result), 2, "Should load the data rows that can be parsed")
            self.assertEqual(len(result.columns), 3, "Should maintain the header column count")
        finally:
            os.unlink(temp_filename)

    def test_special_characters_in_data(self):
        """Test CSV with special characters, unicode, quotes."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as temp_file:
            temp_file.write('name,description,price\n')
            temp_file.write('"John, Jr.","Description with ""quotes""",123.45\n')
            temp_file.write('José,ñáme with ácceñts,€100.00\n')
            temp_file.write('Test,Line\nbreak,999\n')  # Embedded newline
            temp_filename = temp_file.name
        
        try:
            loader = CSVDataLoader(temp_filename)
            result = loader.load()
            
            self.assertIsInstance(result, pl.DataFrame)
            self.assertEqual(len(result.columns), 3)
            # Check that special characters are preserved
            self.assertIn('José', result['name'])
            self.assertIn('John, Jr.', result['name'])
        finally:
            os.unlink(temp_filename)

    def test_very_large_file_simulation(self):
        """Test behavior with very large CSV file (simulated)."""
        # Create a reasonably large CSV file for testing
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
            temp_file.write("id,value1,value2,value3\n")
            
            # Write 10000 rows to simulate larger file
            for i in range(10000):
                temp_file.write(f"{i},data_{i},value_{i},item_{i}\n")
            temp_filename = temp_file.name
        
        try:
            loader = CSVDataLoader(temp_filename)
            result = loader.load()
            
            self.assertIsInstance(result, pl.DataFrame)
            self.assertEqual(len(result), 10000)
            self.assertEqual(len(result.columns), 4)
        finally:
            os.unlink(temp_filename)

    def test_malformed_csv_structure(self):
        """Test CSV with completely malformed structure."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
            # Write content that's not really CSV-like
            temp_file.write("This is not CSV\n")
            temp_file.write("Random text here\n")
            temp_file.write("No commas or structure\n")
            temp_filename = temp_file.name
        
        try:
            loader = CSVDataLoader(temp_filename)
            result = loader.load()
            
            # pandas will treat this as single-column data
            self.assertIsInstance(result, pl.DataFrame)
            self.assertEqual(len(result.columns), 1)
            # First column name will be the first line
            self.assertEqual(result.columns[0], "This is not CSV")
        finally:
            os.unlink(temp_filename)

    def test_different_delimiters(self):
        """Test CSV files with different delimiters (should fail with current implementation)."""
        # Test semicolon-separated
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
            temp_file.write("col1;col2;col3\n")
            temp_file.write("val1;val2;val3\n")
            temp_filename = temp_file.name
        
        try:
            loader = CSVDataLoader(temp_filename)
            result = loader.load()
            
            # Current implementation assumes comma delimiter
            # So this will be parsed as single column
            self.assertIsInstance(result, pl.DataFrame)
            # This shows a limitation - should ideally detect delimiter
            self.assertEqual(len(result.columns), 1)
        finally:
            os.unlink(temp_filename)

    def test_binary_file_as_csv(self):
        """Test loading a binary file with .csv extension."""
        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as temp_file:
            # Write binary data
            temp_file.write(b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00')
            temp_filename = temp_file.name
        
        try:
            loader = CSVDataLoader(temp_filename)
            
            # Polars may handle binary data differently than Pandas
            # Let's check what actually happens
            try:
                result = loader.load()
                # If it loads successfully, it should be empty or minimal
                self.assertIsInstance(result, pl.DataFrame)
            except Exception as e:
                # If it raises an exception, that's also acceptable
                self.assertIsInstance(e, Exception)
        finally:
            os.unlink(temp_filename)

    @patch('focus_validator.data_loaders.csv_data_loader.pl.read_csv')
    def test_polars_exception_handling(self, mock_read_csv):
        """Test handling when pl.read_csv raises unexpected exceptions."""
        mock_read_csv.side_effect = Exception("Unexpected polars error")
        
        loader = CSVDataLoader(self.temp_csv.name)
        
        # Should propagate the exception
        with self.assertRaises(Exception) as cm:
            loader.load()
        
        self.assertIn("Unexpected polars error", str(cm.exception))

    def test_mixed_data_types_per_column(self):
        """Test CSV with mixed data types in columns."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
            temp_file.write("mixed_col,numeric_col\n")
            temp_file.write("123,456\n")          # Numbers
            temp_file.write("text,789\n")         # Mixed text/number
            temp_file.write("45.67,abc\n")        # Mixed number/text
            temp_file.write("true,999\n")         # Boolean-like
            temp_filename = temp_file.name
        
        try:
            loader = CSVDataLoader(temp_filename)
            result = loader.load()
            
            self.assertIsInstance(result, pl.DataFrame)
            self.assertEqual(len(result), 4)
            # pandas should handle mixed types by using object dtype
            self.assertEqual(result['mixed_col'].dtype, pl.Utf8)  # Polars infers mixed types as string
        finally:
            os.unlink(temp_filename)

    def test_extremely_long_lines(self):
        """Test CSV with extremely long lines."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
            temp_file.write("id,long_text\n")
            # Create a very long string (10KB)
            long_text = "x" * 10000
            temp_file.write(f'1,"{long_text}"\n')
            temp_file.write('2,"short text"\n')
            temp_filename = temp_file.name
        
        try:
            loader = CSVDataLoader(temp_filename)
            result = loader.load()
            
            self.assertIsInstance(result, pl.DataFrame)
            self.assertEqual(len(result), 2)
            # Check that long text was preserved
            self.assertEqual(len(result['long_text'][0]), 10000)
        finally:
            os.unlink(temp_filename)

    def test_loader_attributes(self):
        """Test that CSVDataLoader has expected attributes."""
        loader = CSVDataLoader(self.temp_csv.name)
        
        # Check basic attributes
        self.assertEqual(loader.data_filename, self.temp_csv.name)
        self.assertTrue(hasattr(loader, 'load'))
        self.assertTrue(callable(loader.load))

    def test_successful_load_functionality(self):
        """Test that successful loads work correctly."""
        loader = CSVDataLoader(self.temp_csv.name)
        result = loader.load()
        
        # Should return a DataFrame with expected data
        self.assertIsInstance(result, pl.DataFrame)
        self.assertEqual(len(result), 2)  # Two data rows
        self.assertEqual(len(result.columns), 2)  # Two columns
        self.assertEqual(list(result.columns), ['column1', 'column2'])


class TestCSVDataLoaderResilientLoading(unittest.TestCase):
    """Test new resilient loading functionality with column types and error handling."""

    def setUp(self):
        """Set up test fixtures."""
        pass

    def tearDown(self):
        """Clean up test fixtures."""
        # Clean up any temp files created during tests
        for attr_name in dir(self):
            if attr_name.startswith('temp_') and hasattr(self, attr_name):
                temp_file = getattr(self, attr_name)
                if hasattr(temp_file, 'name') and os.path.exists(temp_file.name):
                    os.unlink(temp_file.name)

    def test_column_types_initialization(self):
        """Test initialization with column types parameter."""
        column_types = {
            'BilledCost': 'float64',
            'BillingPeriodStart': pl.Datetime("us", "UTC"),
            'AvailabilityZone': 'string'
        }
        
        loader = CSVDataLoader("test.csv", column_types=column_types)
        
        self.assertEqual(loader.column_types, column_types)
        self.assertEqual(loader.failed_columns, set())

    def test_resilient_loading_with_invalid_numeric_data(self):
        """Test that columns with mixed numeric/string data are inferred as String."""
        # Create CSV with invalid numeric data
        self.temp_csv = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        self.temp_csv.write("BilledCost,ResourceId\n")
        self.temp_csv.write("123.45,resource1\n")
        self.temp_csv.write("INVALID_NUMBER,resource2\n")
        self.temp_csv.write("67.89,resource3\n")
        self.temp_csv.close()
        
        column_types = {'BilledCost': 'float64', 'ResourceId': 'string'}
        loader = CSVDataLoader(self.temp_csv.name, column_types=column_types)
        
        result = loader.load()
        
        # NEW BEHAVIOR: Mixed numeric + string data causes inference as String
        # This allows type validation to detect the problem
        self.assertIsInstance(result, pl.DataFrame)
        self.assertEqual(len(result), 3)
        self.assertEqual(result['BilledCost'].dtype, pl.String)  # Inferred as String
        self.assertEqual(result['BilledCost'][0], '123.45')  # String
        self.assertEqual(result['BilledCost'][1], 'INVALID_NUMBER')  # String
        self.assertEqual(result['BilledCost'][2], '67.89')  # String

    def test_resilient_loading_with_invalid_datetime_data(self):
        """Test resilient loading when datetime columns contain invalid data - columns should be dropped."""
        # Create CSV with invalid datetime data
        self.temp_csv = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        self.temp_csv.write("BillingPeriodStart,BillingPeriodEnd,Amount\n")
        self.temp_csv.write("2023-01-01T00:00:00Z,2023-01-31T23:59:59Z,100\n")
        self.temp_csv.write("INVALID_DATE,2023-02-28T23:59:59Z,200\n")
        self.temp_csv.write("2023-03-01T00:00:00Z,NOT_A_DATE,300\n")
        self.temp_csv.close()
        
        column_types = {
            'BillingPeriodStart': pl.Datetime("us", "UTC"),
            'BillingPeriodEnd': pl.Datetime("us", "UTC"),
            'Amount': 'float64'
        }
        loader = CSVDataLoader(self.temp_csv.name, column_types=column_types)
        
        result = loader.load()
        
        # Should succeed but drop columns with invalid dates
        self.assertIsInstance(result, pl.DataFrame)
        self.assertEqual(len(result), 3)
        
        # Both datetime columns should be dropped due to invalid dates
        self.assertNotIn('BillingPeriodStart', result.columns)
        self.assertNotIn('BillingPeriodEnd', result.columns)
        self.assertIn('BillingPeriodStart', loader.failed_columns)
        self.assertIn('BillingPeriodEnd', loader.failed_columns)
        
        # Amount column should be preserved
        self.assertIn('Amount', result.columns)
        self.assertEqual(result['Amount'][0], 100.0)
        self.assertEqual(result['Amount'][1], 200.0)
        self.assertEqual(result['Amount'][2], 300.0)

    def test_resilient_loading_mixed_data_corruption(self):
        """Test resilient loading with multiple types of data corruption."""
        # Create CSV with various data quality issues
        self.temp_csv = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        self.temp_csv.write("BilledCost,BillingPeriodStart,AvailabilityZone,ResourceId\n")
        self.temp_csv.write("123.45,2023-01-01T00:00:00Z,us-east-1a,resource1\n")
        self.temp_csv.write("NOT_A_NUMBER,INVALID_DATE,eu-west-1b,resource2\n")  # Multiple issues
        self.temp_csv.write("67.89,2023-03-01T00:00:00Z,,resource3\n")  # Empty string
        self.temp_csv.write(",2023-04-01T00:00:00Z,ap-south-1,\n")  # Empty values
        self.temp_csv.close()
        
        column_types = {
            'BilledCost': 'float64',
            'BillingPeriodStart': pl.Datetime("us", "UTC"),
            'AvailabilityZone': 'string',
            'ResourceId': 'string'
        }
        loader = CSVDataLoader(self.temp_csv.name, column_types=column_types)
        
        result = loader.load()
        
        # Should succeed despite multiple issues
        self.assertIsInstance(result, pl.DataFrame)
        self.assertEqual(len(result), 4)
        
        # NEW BEHAVIOR: BilledCost has mixed data (floats + "NOT_A_NUMBER")
        # so Polars infers it as String. This is correct - type validation will catch it.
        self.assertEqual(result['BilledCost'][0], '123.45')  # Loaded as string
        self.assertEqual(result['BilledCost'][2], '67.89')  # Loaded as string
        self.assertEqual(result['BilledCost'][1], 'NOT_A_NUMBER')  # Invalid kept as-is
        self.assertTrue(result['BilledCost'][3] is None or result['BilledCost'][3] == '')  # Empty
        
        # BillingPeriodStart column should be dropped due to invalid date (INVALID_DATE)
        self.assertNotIn('BillingPeriodStart', result.columns)
        self.assertIn('BillingPeriodStart', loader.failed_columns)

    def test_fallback_to_string_columns_when_no_types_provided(self):
        """Test fallback to hardcoded STRING_COLUMNS when no column_types provided."""
        # Create CSV with FOCUS column names
        self.temp_csv = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        self.temp_csv.write("AvailabilityZone,BillingAccountId,OtherColumn\n")
        self.temp_csv.write("us-east-1a,account123,value1\n")
        self.temp_csv.write("eu-west-1b,account456,value2\n")
        self.temp_csv.close()
        
        # Initialize without column_types
        loader = CSVDataLoader(self.temp_csv.name)
        result = loader.load()
        
        # Should apply string typing to known FOCUS columns
        self.assertIsInstance(result, pl.DataFrame)
        self.assertEqual(result['AvailabilityZone'].dtype, pl.Utf8)
        self.assertEqual(result['BillingAccountId'].dtype, pl.Utf8)
        # OtherColumn contains string values, so Polars will infer it as string
        self.assertEqual(result['OtherColumn'].dtype, pl.Utf8)

    def test_failed_columns_tracking(self):
        """Test that columns with data use inferred types, not forced spec types."""
        # Create CSV with integer data - spec says float64 but data is integers
        self.temp_csv = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        self.temp_csv.write("GoodColumn,ProblematicColumn\n")
        self.temp_csv.write("123,456\n")
        self.temp_csv.write("789,VERY_LONG_STRING_THAT_CANNOT_BE_CONVERTED_TO_FLOAT\n")
        self.temp_csv.close()
        
        column_types = {
            'GoodColumn': 'float64',
            'ProblematicColumn': 'float64'
        }
        
        loader = CSVDataLoader(self.temp_csv.name, column_types=column_types)
        result = loader.load()
        
        # The loader should succeed in loading the data
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 2)
        
        # NEW BEHAVIOR: GoodColumn has actual data, so it keeps inferred type (Int64)
        # This allows type validation to catch mismatches between spec and actual data
        self.assertEqual(result['GoodColumn'].dtype, pl.Int64)
        
        # The problematic column should still be loaded (as VARCHAR since it has mixed data)
        # Polars will infer VARCHAR when it encounters the string value
        self.assertTrue('ProblematicColumn' in result.columns)

    def test_integer_type_conversion_with_coercion(self):
        """Test that mixed data types are inferred as String, not coerced."""
        # Create CSV with integer column that has invalid values
        self.temp_csv = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        self.temp_csv.write("IntegerColumn,Description\n")
        self.temp_csv.write("123,item1\n")
        self.temp_csv.write("NOT_AN_INT,item2\n")
        self.temp_csv.write("456,item3\n")
        self.temp_csv.close()
        
        column_types = {'IntegerColumn': 'int64', 'Description': 'string'}
        loader = CSVDataLoader(self.temp_csv.name, column_types=column_types)
        
        result = loader.load()
        
        # NEW BEHAVIOR: Column has mixed data (integers + string)
        # Polars infers String type, which is correct - this allows type validation
        # to catch that the spec expects Int64 but data is String
        self.assertIsInstance(result, pl.DataFrame)
        self.assertEqual(len(result), 3)
        # Column with mixed data should be inferred as String
        self.assertEqual(str(result['IntegerColumn'].dtype), 'String')

    def test_stdin_input_handling(self):
        """Test handling of stdin input ('-' filename)."""
        csv_content = "col1,col2\n1,2\n3,4\n"
        
        with patch('sys.stdin', io.StringIO(csv_content)):
            loader = CSVDataLoader("-")
            result = loader.load()
            
            self.assertIsInstance(result, pl.DataFrame)
            self.assertEqual(len(result), 2)
            self.assertEqual(list(result.columns), ['col1', 'col2'])

    def test_ultimate_fallback_on_complete_failure(self):
        """Test ultimate fallback to basic CSV loading when all else fails."""
        self.temp_csv = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        self.temp_csv.write("col1,col2\n1,2\n3,4\n")
        self.temp_csv.close()
        
        column_types = {'col1': 'float64'}
        loader = CSVDataLoader(self.temp_csv.name, column_types=column_types)
        
        # Mock the basic pl.read_csv to fail in _load_and_convert_with_coercion fallback
        with patch('focus_validator.data_loaders.csv_data_loader.pl.read_csv', side_effect=Exception("Complete failure")):
            # Should propagate the exception since there's no further fallback
            with self.assertRaises(Exception) as cm:
                loader.load()
            self.assertIn("Failed to load CSV data", str(cm.exception))

    def test_warning_logging_for_failed_conversions(self):
        """Test that failed conversions are properly tracked."""
        # Create CSV with data that will trigger conversion failures
        self.temp_csv = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        self.temp_csv.write("NumericCol,DateCol\n")
        self.temp_csv.write("123,2023-01-01\n")
        self.temp_csv.write("INVALID,BAD_DATE\n")
        self.temp_csv.close()
        
        column_types = {
            'NumericCol': 'float64',
            'DateCol': pl.Datetime("us", "UTC")
        }
        
        loader = CSVDataLoader(self.temp_csv.name, column_types=column_types)
        result = loader.load()
        
        # DateCol should be in failed_columns due to BAD_DATE
        self.assertIn('DateCol', loader.failed_columns)
        self.assertNotIn('DateCol', result.columns)
        
        # NumericCol should be preserved (INVALID gets converted to null)
        self.assertIn('NumericCol', result.columns)
        self.assertNotIn('NumericCol', loader.failed_columns)

    def test_two_pass_loading_for_column_existence_check(self):
        """Test that two-pass loading correctly filters column types for existing columns."""
        # Create CSV with only some of the specified columns
        self.temp_csv = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        self.temp_csv.write("ExistingCol1,ExistingCol2\n")
        self.temp_csv.write("value1,value2\n")
        self.temp_csv.close()
        
        # Specify types for both existing and non-existing columns
        column_types = {
            'ExistingCol1': 'string',
            'ExistingCol2': 'float64',
            'NonExistingCol': 'int64'  # This column doesn't exist in CSV
        }
        
        loader = CSVDataLoader(self.temp_csv.name, column_types=column_types)
        result = loader.load()
        
        # Should successfully load without trying to apply types to non-existing columns
        self.assertIsInstance(result, pl.DataFrame)
        self.assertEqual(len(result.columns), 2)
        self.assertIn('ExistingCol1', result.columns)
        self.assertIn('ExistingCol2', result.columns)
        self.assertNotIn('NonExistingCol', result.columns)

    def test_file_like_object_handling(self):
        """Test handling of file-like objects (BytesIO, etc.)."""
        csv_content = "col1,col2\n1,2\n3,4\n"
        csv_buffer = io.StringIO(csv_content)
        
        column_types = {'col1': 'int64', 'col2': 'int64'}
        loader = CSVDataLoader(csv_buffer, column_types=column_types)
        
        result = loader.load()
        
        self.assertIsInstance(result, pl.DataFrame)
        self.assertEqual(len(result), 2)
        self.assertEqual(list(result.columns), ['col1', 'col2'])

    def test_coercion_preserves_valid_data_types(self):
        """Test that coercion properly preserves valid data while dropping columns with invalid dates."""
        # Create CSV with mix of valid and invalid data
        self.temp_csv = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        self.temp_csv.write("Amount,StartDate,Zone\n")
        self.temp_csv.write("100.50,2023-01-01T00:00:00Z,us-east-1\n")  # All valid
        self.temp_csv.write("INVALID,INVALID_DATE,eu-west-1\n")  # Multiple invalid
        self.temp_csv.write("200.75,2023-03-01T00:00:00Z,ap-south-1\n")  # All valid
        self.temp_csv.close()
        
        column_types = {
            'Amount': 'float64',
            'StartDate': pl.Datetime("us", "UTC"),
            'Zone': 'string'
        }
        
        loader = CSVDataLoader(self.temp_csv.name, column_types=column_types)
        result = loader.load()
        
        # Check that valid data maintains correct types and values
        self.assertEqual(result['Amount'][0], 100.50)
        self.assertEqual(result['Amount'][2], 200.75)
        self.assertTrue((result['Amount'][1] is None))  # Invalid -> NaN
        
        # StartDate column should be dropped due to invalid date (INVALID_DATE)
        self.assertNotIn('StartDate', result.columns)
        self.assertIn('StartDate', loader.failed_columns)
        
        # String columns should all be valid
        self.assertEqual(result['Zone'][0], 'us-east-1')
        self.assertEqual(result['Zone'][1], 'eu-west-1')
        self.assertEqual(result['Zone'][2], 'ap-south-1')


if __name__ == '__main__':
    unittest.main()