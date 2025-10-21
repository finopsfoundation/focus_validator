"""Comprehensive tests for Parquet data loader functionality and resilient loading."""

import unittest
from unittest.mock import Mock, patch
import tempfile
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import sys

from focus_validator.data_loaders.parquet_data_loader import ParquetDataLoader


class TestParquetDataLoaderBasic(unittest.TestCase):
    """Test basic Parquet data loader functionality."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a valid Parquet file for testing
        self.temp_parquet = tempfile.NamedTemporaryFile(suffix='.parquet', delete=False)
        self.temp_parquet.close()
        
        # Create test data and save as Parquet
        test_data = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c'],
            'col3': [1.1, 2.2, 3.3]
        })
        test_data.to_parquet(self.temp_parquet.name, index=False)

    def tearDown(self):
        """Clean up test fixtures."""
        if os.path.exists(self.temp_parquet.name):
            os.unlink(self.temp_parquet.name)

    @patch('focus_validator.data_loaders.parquet_data_loader.pd.read_parquet')
    def test_file_not_found(self, mock_read_parquet):
        """Test behavior when Parquet file doesn't exist."""
        mock_read_parquet.side_effect = FileNotFoundError("File not found")
        
        loader = ParquetDataLoader("nonexistent_file.parquet")
        
        with self.assertRaises(FileNotFoundError):
            loader.load()

    def test_corrupted_parquet_file(self):
        """Test loading a corrupted Parquet file."""
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            # Write invalid parquet data (just random bytes)
            temp_file.write(b'This is not parquet data\x00\x01\x02\x03')
            temp_filename = temp_file.name
        
        try:
            loader = ParquetDataLoader(temp_filename)
            
            # Should raise some kind of exception when trying to read invalid parquet
            with self.assertRaises(Exception):  # Broad catch for various parquet errors
                loader.load()
        finally:
            os.unlink(temp_filename)

    @patch('focus_validator.data_loaders.parquet_data_loader.pd.read_parquet')
    def test_pandas_exception_handling(self, mock_read_parquet):
        """Test handling when pandas.read_parquet raises unexpected exceptions."""
        mock_read_parquet.side_effect = Exception("Unexpected pandas error")
        
        loader = ParquetDataLoader("test.parquet")
        
        # Should propagate the exception
        with self.assertRaises(Exception) as cm:
            loader.load()
        
        self.assertIn("Unexpected pandas error", str(cm.exception))

    @patch('focus_validator.data_loaders.parquet_data_loader.pd.read_parquet')
    def test_successful_mocked_load(self, mock_read_parquet):
        """Test successful parquet loading with mocked pandas."""
        # Mock successful parquet loading
        expected_df = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c']
        })
        mock_read_parquet.return_value = expected_df
        
        loader = ParquetDataLoader("test.parquet")
        result = loader.load()
        
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 3)
        self.assertEqual(len(result.columns), 2)
        mock_read_parquet.assert_called_once_with("test.parquet")

    def test_loader_attributes(self):
        """Test that ParquetDataLoader has expected attributes."""
        loader = ParquetDataLoader("test.parquet")
        
        # Check basic attributes
        self.assertEqual(loader.data_filename, "test.parquet")
        self.assertTrue(hasattr(loader, 'load'))
        self.assertTrue(callable(loader.load))

    def test_initialization_with_different_filenames(self):
        """Test initialization with various filename patterns."""
        test_files = [
            "data.parquet",
            "/path/to/data.parquet",
            "data_with_underscores.parquet",
            "data-with-hyphens.parquet",
            "data123.parquet"
        ]
        
        for filename in test_files:
            with self.subTest(filename=filename):
                loader = ParquetDataLoader(filename)
                self.assertEqual(loader.data_filename, filename)

    @patch('focus_validator.data_loaders.parquet_data_loader.pd.read_parquet')
    def test_load_returns_none_handling(self, mock_read_parquet):
        """Test handling when pandas returns None (unusual but possible)."""
        mock_read_parquet.return_value = None
        
        loader = ParquetDataLoader("test.parquet")
        result = loader.load()
        
        self.assertIsNone(result)

    def test_binary_file_as_parquet(self):
        """Test loading a non-parquet binary file with .parquet extension."""
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            # Write PNG header as binary data
            temp_file.write(b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00')
            temp_filename = temp_file.name
        
        try:
            loader = ParquetDataLoader(temp_filename)
            
            # This should raise some kind of format-related exception
            with self.assertRaises(Exception):
                loader.load()
        finally:
            os.unlink(temp_filename)

    def test_empty_file_as_parquet(self):
        """Test loading completely empty file with parquet extension."""
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            # File is created but remains empty
            temp_filename = temp_file.name
        
        try:
            loader = ParquetDataLoader(temp_filename)
            
            # Empty file should raise an exception when pandas tries to read it
            with self.assertRaises(Exception):
                loader.load()
        finally:
            os.unlink(temp_filename)


class TestParquetDataLoaderResilientLoading(unittest.TestCase):
    """Test resilient loading functionality with column types and error handling."""

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
            'BillingPeriodStart': 'datetime64[ns, UTC]',
            'AvailabilityZone': 'string'
        }
        
        loader = ParquetDataLoader("test.parquet", column_types=column_types)
        
        self.assertEqual(loader.column_types, column_types)
        self.assertEqual(loader.failed_columns, set())

    def test_successful_load_with_type_conversion(self):
        """Test successful loading with column type conversions."""
        # Create test Parquet data
        test_data = pd.DataFrame({
            'Amount': [100.5, 200.0, 300.75],
            'Date': ['2023-01-01', '2023-02-01', '2023-03-01'],
            'Zone': ['us-east-1a', 'eu-west-1b', 'ap-south-1c']
        })
        
        self.temp_parquet = tempfile.NamedTemporaryFile(suffix='.parquet', delete=False)
        self.temp_parquet.close()
        test_data.to_parquet(self.temp_parquet.name, index=False)
        
        # Define column types for conversion
        column_types = {
            'Amount': 'float64',
            'Date': 'datetime64[ns, UTC]',
            'Zone': 'string'
        }
        
        loader = ParquetDataLoader(self.temp_parquet.name, column_types=column_types)
        result = loader.load()
        
        # Check successful loading and type conversion
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 3)
        self.assertEqual(result['Amount'].dtype, 'float64')
        self.assertEqual(str(result['Date'].dtype), 'datetime64[ns, UTC]')
        self.assertEqual(result['Zone'].dtype.name, 'string')

    def test_type_conversion_with_invalid_data(self):
        """Test type conversion resilience with data that can't be converted."""
        # Create Parquet data with mixed types that will challenge conversion
        test_data = pd.DataFrame({
            'NumericCol': ['123.45', 'INVALID', '67.89'],  # String column with invalid numeric
            'DateCol': ['2023-01-01', 'BAD_DATE', '2023-03-01'],  # String column with invalid date
            'StringCol': [1, 2, 3]  # Numeric column to convert to string
        })
        
        self.temp_parquet = tempfile.NamedTemporaryFile(suffix='.parquet', delete=False)
        self.temp_parquet.close()
        test_data.to_parquet(self.temp_parquet.name, index=False)
        
        column_types = {
            'NumericCol': 'float64',
            'DateCol': 'datetime64[ns, UTC]',
            'StringCol': 'string'
        }
        
        loader = ParquetDataLoader(self.temp_parquet.name, column_types=column_types)
        result = loader.load()
        
        # Should succeed with coercion
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 3)
        
        # Check numeric coercion
        self.assertEqual(result['NumericCol'].iloc[0], 123.45)
        self.assertTrue(pd.isna(result['NumericCol'].iloc[1]))  # Invalid -> NaN
        self.assertEqual(result['NumericCol'].iloc[2], 67.89)
        
        # Check datetime coercion
        self.assertIsInstance(result['DateCol'].iloc[0], pd.Timestamp)
        self.assertTrue(pd.isna(result['DateCol'].iloc[1]))  # Invalid -> NaT
        self.assertIsInstance(result['DateCol'].iloc[2], pd.Timestamp)
        
        # Check string conversion
        self.assertEqual(result['StringCol'].dtype.name, 'string')

    def test_stdin_input_handling(self):
        """Test handling of stdin input ('-' filename)."""
        # Create test Parquet data in memory
        test_data = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c']
        })
        
        # Convert to Parquet bytes
        parquet_buffer = io.BytesIO()
        test_data.to_parquet(parquet_buffer, index=False)
        parquet_bytes = parquet_buffer.getvalue()
        
        # Mock sys.stdin.buffer.read() to return our Parquet data
        with patch('sys.stdin.buffer.read', return_value=parquet_bytes):
            loader = ParquetDataLoader("-")
            result = loader.load()
            
            self.assertIsInstance(result, pd.DataFrame)
            self.assertEqual(len(result), 3)
            self.assertEqual(list(result.columns), ['col1', 'col2'])

    def test_no_column_types_provided(self):
        """Test loading without any column type specifications."""
        test_data = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c']
        })
        
        self.temp_parquet = tempfile.NamedTemporaryFile(suffix='.parquet', delete=False)
        self.temp_parquet.close()
        test_data.to_parquet(self.temp_parquet.name, index=False)
        
        loader = ParquetDataLoader(self.temp_parquet.name)
        result = loader.load()
        
        # Should load successfully with original Parquet types
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 3)
        self.assertEqual(len(result.columns), 2)


if __name__ == '__main__':
    unittest.main()