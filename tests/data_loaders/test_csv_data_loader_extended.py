"""Comprehensive tests for CSV data loader error handling and edge cases."""

import unittest
from unittest.mock import Mock, patch, mock_open
import tempfile
import os
import pandas as pd
import io

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
        
        with self.assertRaises(FileNotFoundError):
            loader.load()

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
                with self.assertRaises(PermissionError):
                    loader.load()
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
            
            # pandas raises EmptyDataError for completely empty files
            with self.assertRaises(pd.errors.EmptyDataError):
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
            
            self.assertIsInstance(result, pd.DataFrame)
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
            
            # pandas raises ParserError for inconsistent column counts
            with self.assertRaises(pd.errors.ParserError):
                loader.load()
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
            
            self.assertIsInstance(result, pd.DataFrame)
            self.assertEqual(len(result.columns), 3)
            # Check that special characters are preserved
            self.assertIn('José', result['name'].values)
            self.assertIn('John, Jr.', result['name'].values)
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
            
            self.assertIsInstance(result, pd.DataFrame)
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
            self.assertIsInstance(result, pd.DataFrame)
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
            self.assertIsInstance(result, pd.DataFrame)
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
            
            # This should raise a UnicodeDecodeError or similar
            with self.assertRaises((UnicodeDecodeError, pd.errors.ParserError)):
                loader.load()
        finally:
            os.unlink(temp_filename)

    @patch('focus_validator.data_loaders.csv_data_loader.pd.read_csv')
    def test_pandas_exception_handling(self, mock_read_csv):
        """Test handling when pandas.read_csv raises unexpected exceptions."""
        mock_read_csv.side_effect = Exception("Unexpected pandas error")
        
        loader = CSVDataLoader(self.temp_csv.name)
        
        # Should propagate the exception
        with self.assertRaises(Exception) as cm:
            loader.load()
        
        self.assertIn("Unexpected pandas error", str(cm.exception))

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
            
            self.assertIsInstance(result, pd.DataFrame)
            self.assertEqual(len(result), 4)
            # pandas should handle mixed types by using object dtype
            self.assertEqual(result['mixed_col'].dtype, 'object')
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
            
            self.assertIsInstance(result, pd.DataFrame)
            self.assertEqual(len(result), 2)
            # Check that long text was preserved
            self.assertEqual(len(result.iloc[0]['long_text']), 10000)
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
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 2)  # Two data rows
        self.assertEqual(len(result.columns), 2)  # Two columns
        self.assertEqual(list(result.columns), ['column1', 'column2'])


if __name__ == '__main__':
    unittest.main()