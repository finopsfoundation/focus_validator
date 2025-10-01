"""Basic tests for Parquet data loader functionality."""

import unittest
from unittest.mock import Mock, patch
import tempfile
import os
import pandas as pd

from focus_validator.data_loaders.parquet_data_loader import ParquetDataLoader


class TestParquetDataLoaderBasic(unittest.TestCase):
    """Test basic Parquet data loader functionality."""

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


if __name__ == '__main__':
    unittest.main()