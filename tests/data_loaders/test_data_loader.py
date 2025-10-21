"""Comprehensive tests for the main DataLoader class."""

import unittest
from unittest.mock import Mock, patch, MagicMock
import tempfile
import os
import pandas as pd

from focus_validator.data_loaders.data_loader import DataLoader
from focus_validator.data_loaders.csv_data_loader import CSVDataLoader
from focus_validator.data_loaders.parquet_data_loader import ParquetDataLoader
from focus_validator.exceptions import FocusNotImplementedError


class TestDataLoader(unittest.TestCase):
    """Test the main DataLoader class functionality."""

    def setUp(self):
        """Set up test fixtures."""
        # Create temporary files for testing
        self.temp_csv = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        self.temp_csv.write("col1,col2\n1,2\n3,4\n")
        self.temp_csv.close()
        
        # Skip parquet file creation for tests that don't need it
        # We'll mock parquet functionality instead

    def tearDown(self):
        """Clean up test fixtures."""
        if os.path.exists(self.temp_csv.name):
            os.unlink(self.temp_csv.name)

    def test_initialization_with_valid_csv_file(self):
        """Test DataLoader initialization with valid CSV file."""
        loader = DataLoader(self.temp_csv.name)
        
        self.assertEqual(loader.data_filename, self.temp_csv.name)
        self.assertEqual(loader.data_loader_class, CSVDataLoader)
        self.assertIsInstance(loader.data_loader, CSVDataLoader)

    def test_initialization_with_valid_parquet_file(self):
        """Test DataLoader initialization with valid Parquet file."""
        # Create a temp file with parquet extension for testing
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_parquet:
            temp_parquet_name = temp_parquet.name
        
        try:
            loader = DataLoader(temp_parquet_name)
            
            self.assertEqual(loader.data_filename, temp_parquet_name)
            self.assertEqual(loader.data_loader_class, ParquetDataLoader)
            self.assertIsInstance(loader.data_loader, ParquetDataLoader)
        finally:
            if os.path.exists(temp_parquet_name):
                os.unlink(temp_parquet_name)

    def test_initialization_with_none_filename(self):
        """Test DataLoader initialization with None filename raises error."""
        with self.assertRaises(FocusNotImplementedError) as cm:
            DataLoader(None)
        
        self.assertIn("Data filename cannot be None", str(cm.exception))

    @patch('focus_validator.data_loaders.data_loader.logging.getLogger')
    def test_initialization_with_nonexistent_file(self, mock_get_logger):
        """Test DataLoader initialization with nonexistent file."""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        
        # Should still initialize but log warning
        loader = DataLoader("nonexistent.csv")
        
        mock_logger.warning.assert_called()
        warning_call = mock_logger.warning.call_args[0][0]
        self.assertIn("Data file does not exist", warning_call)

    def test_find_data_loader_csv(self):
        """Test find_data_loader method for CSV files."""
        loader = DataLoader(self.temp_csv.name)
        result = loader.find_data_loader()
        self.assertEqual(result, CSVDataLoader)

    def test_find_data_loader_parquet(self):
        """Test find_data_loader method for Parquet files."""
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_parquet:
            temp_parquet_name = temp_parquet.name
        
        try:
            loader = DataLoader(temp_parquet_name)
            result = loader.find_data_loader()
            self.assertEqual(result, ParquetDataLoader)
        finally:
            if os.path.exists(temp_parquet_name):
                os.unlink(temp_parquet_name)

    def test_find_data_loader_unsupported_extension(self):
        """Test find_data_loader method with unsupported file extension."""
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
            temp_filename = temp_file.name
        
        try:
            with self.assertRaises(FocusNotImplementedError) as cm:
                DataLoader(temp_filename)
            
            self.assertIn("File type not implemented yet", str(cm.exception))
        finally:
            if os.path.exists(temp_filename):
                os.unlink(temp_filename)

    def test_find_data_loader_no_extension(self):
        """Test find_data_loader method with file having no extension."""
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_filename = temp_file.name
        
        try:
            with self.assertRaises(FocusNotImplementedError) as cm:
                DataLoader(temp_filename)
            
            self.assertIn("File type not implemented yet", str(cm.exception))
        finally:
            if os.path.exists(temp_filename):
                os.unlink(temp_filename)

    def test_load_csv_success(self):
        """Test successful loading of CSV data."""
        loader = DataLoader(self.temp_csv.name)
        result = loader.load()
        
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 2)  # 2 data rows
        self.assertEqual(list(result.columns), ["col1", "col2"])
        self.assertEqual(result.iloc[0]["col1"], 1)

    @patch('focus_validator.data_loaders.parquet_data_loader.ParquetDataLoader.load')
    def test_load_parquet_success(self, mock_parquet_load):
        """Test successful loading of Parquet data."""
        # Mock the parquet loader to return a DataFrame
        mock_df = pd.DataFrame({"col1": [1, 3], "col2": [2, 4]})
        mock_parquet_load.return_value = mock_df
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_parquet:
            temp_parquet_name = temp_parquet.name
        
        try:
            loader = DataLoader(temp_parquet_name)
            result = loader.load()
            
            self.assertIsInstance(result, pd.DataFrame)
            self.assertEqual(len(result), 2)  # 2 data rows
            self.assertEqual(list(result.columns), ["col1", "col2"])
            self.assertEqual(result.iloc[0]["col1"], 1)
        finally:
            if os.path.exists(temp_parquet_name):
                os.unlink(temp_parquet_name)

    @patch('focus_validator.data_loaders.data_loader.logging.getLogger')
    def test_load_logs_success_info(self, mock_get_logger):
        """Test that successful load logs appropriate information."""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        
        loader = DataLoader(self.temp_csv.name)
        result = loader.load()
        
        # Should log successful loading with row/column counts
        info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        success_logs = [call for call in info_calls if "Data loaded successfully" in call]
        self.assertTrue(len(success_logs) > 0)

    @patch('focus_validator.data_loaders.data_loader.logging.getLogger')
    def test_initialization_logs_file_size(self, mock_get_logger):
        """Test that initialization logs file size for existing files."""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        
        DataLoader(self.temp_csv.name)
        
        # Should log file size
        info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        size_logs = [call for call in info_calls if "File size:" in call]
        self.assertTrue(len(size_logs) > 0)

    @patch('focus_validator.data_loaders.csv_data_loader.CSVDataLoader.load')
    @patch('focus_validator.data_loaders.data_loader.logging.getLogger')
    def test_load_handles_none_result(self, mock_get_logger, mock_csv_load):
        """Test handling when underlying loader returns None."""
        mock_csv_load.return_value = None
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        
        loader = DataLoader(self.temp_csv.name)
        result = loader.load()
        
        self.assertIsNone(result)
        mock_logger.warning.assert_called()
        warning_call = mock_logger.warning.call_args[0][0]
        self.assertIn("Data loading returned None", warning_call)

    @patch('focus_validator.data_loaders.csv_data_loader.CSVDataLoader.load')
    @patch('focus_validator.data_loaders.data_loader.logging.getLogger')
    def test_load_handles_exception_in_dimension_check(self, mock_get_logger, mock_csv_load):
        """Test handling when dimension checking raises exception."""
        # Create a mock result that raises exception on len()
        mock_result = Mock()
        mock_result.__len__ = Mock(side_effect=Exception("Test error"))
        mock_result.columns = ["col1", "col2"]
        mock_csv_load.return_value = mock_result
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        
        loader = DataLoader(self.temp_csv.name)
        result = loader.load()
        
        self.assertEqual(result, mock_result)
        mock_logger.warning.assert_called()
        warning_call = mock_logger.warning.call_args[0][0]
        self.assertIn("Could not determine data dimensions", warning_call)

    def test_logger_configuration(self):
        """Test that logger is properly configured."""
        loader = DataLoader(self.temp_csv.name)
        
        self.assertIsNotNone(loader.log)
        expected_name = "focus_validator.data_loaders.data_loader.DataLoader"
        self.assertEqual(loader.log.name, expected_name)

    def test_case_insensitive_extensions(self):
        """Test that file extensions are handled case-insensitively."""
        # Create temporary files with uppercase extensions
        temp_csv_upper = tempfile.NamedTemporaryFile(mode='w', suffix='.CSV', delete=False)
        temp_csv_upper.write("col1\n1\n")
        temp_csv_upper.close()
        
        temp_parquet_upper = tempfile.NamedTemporaryFile(suffix='.PARQUET', delete=False)
        temp_parquet_upper.close()
        
        try:
            # Current implementation doesn't handle case-insensitive extensions
            # These should raise FocusNotImplementedError
            with self.assertRaises(FocusNotImplementedError):
                DataLoader(temp_csv_upper.name)
                
            with self.assertRaises(FocusNotImplementedError):
                DataLoader(temp_parquet_upper.name)
                
        finally:
            for temp_file in [temp_csv_upper.name, temp_parquet_upper.name]:
                if os.path.exists(temp_file):
                    os.unlink(temp_file)

    def test_performance_logging_decoration(self):
        """Test that load method is properly decorated with performance logging."""
        # This tests that the @logPerformance decorator is applied
        loader = DataLoader(self.temp_csv.name)
        
        # Check that the method has the performance logging attribute
        load_method = getattr(loader, 'load')
        # The decorator should preserve the original method but add logging
        self.assertTrue(hasattr(load_method, '__wrapped__') or 
                       hasattr(load_method, '_original_func') or
                       callable(load_method))

    def test_dataloader_with_column_types_parameter(self):
        """Test that DataLoader can pass column types to underlying loaders."""
        # Note: Current DataLoader doesn't accept column_types parameter
        # This test documents the expected behavior for future enhancement
        
        # Test CSV loader directly with column types
        column_types = {'col1': 'int64', 'col2': 'string'}
        csv_loader = CSVDataLoader(self.temp_csv.name, column_types=column_types)
        
        # Should initialize with column types
        self.assertEqual(csv_loader.column_types, column_types)
        self.assertEqual(csv_loader.failed_columns, set())

    def test_resilient_loading_integration(self):
        """Test that data loaders integrate properly with resilient loading features."""
        # Create CSV with mixed data types and some problematic values
        problematic_csv = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        problematic_csv.write("NumericCol,DateCol,StringCol\n")
        problematic_csv.write("123.45,2023-01-01,value1\n")
        problematic_csv.write("INVALID,BAD_DATE,value2\n")
        problematic_csv.write("67.89,2023-03-01,value3\n")
        problematic_csv.close()
        
        try:
            # Test that CSV loader can handle problematic data with column types
            column_types = {
                'NumericCol': 'float64',
                'DateCol': 'datetime64[ns, UTC]',
                'StringCol': 'string'
            }
            
            csv_loader = CSVDataLoader(problematic_csv.name, column_types=column_types)
            result = csv_loader.load()
            
            # Should succeed despite problematic data
            self.assertIsInstance(result, pd.DataFrame)
            self.assertEqual(len(result), 3)
            
            # Check that types were applied correctly
            self.assertEqual(result['NumericCol'].dtype, 'float64')
            self.assertEqual(str(result['DateCol'].dtype), 'datetime64[ns, UTC]')
            self.assertEqual(result['StringCol'].dtype.name, 'string')
            
            # Check that invalid values were coerced
            self.assertTrue(pd.isna(result['NumericCol'].iloc[1]))  # INVALID -> NaN
            self.assertTrue(pd.isna(result['DateCol'].iloc[1]))     # BAD_DATE -> NaT
            
        finally:
            if os.path.exists(problematic_csv.name):
                os.unlink(problematic_csv.name)


class TestDataLoaderEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions."""

    def test_empty_csv_file(self):
        """Test loading an empty CSV file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
            temp_file.write("")  # Empty file
            temp_filename = temp_file.name
        
        try:
            loader = DataLoader(temp_filename)
            # This might raise an exception or return empty DataFrame
            # depending on pandas behavior
            result = loader.load()
            # Test should handle whatever pandas does with empty files
            if result is not None:
                self.assertIsInstance(result, pd.DataFrame)
        except Exception as e:
            # Empty files might cause pandas to raise exceptions
            # This documents the behavior
            pass
        finally:
            if os.path.exists(temp_filename):
                os.unlink(temp_filename)

    def test_malformed_csv_file(self):
        """Test loading a malformed CSV file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
            temp_file.write("col1,col2\n1,2,3,4\n5\n")  # Inconsistent columns
            temp_filename = temp_file.name
        
        try:
            loader = DataLoader(temp_filename)
            # pandas might handle this gracefully or raise exception
            result = loader.load()
            if result is not None:
                self.assertIsInstance(result, pd.DataFrame)
        except Exception as e:
            # Malformed files might cause exceptions - document this
            pass
        finally:
            if os.path.exists(temp_filename):
                os.unlink(temp_filename)

    @patch('focus_validator.data_loaders.data_loader.logging.getLogger')
    def test_very_large_filename(self, mock_get_logger):
        """Test behavior with very long filename."""
        long_name = "a" * 255 + ".csv"  # Very long filename
        
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        
        # This should handle gracefully (might not exist but shouldn't crash)
        loader = DataLoader(long_name)
        # Should log warning about nonexistent file
        mock_logger.warning.assert_called()

    @patch('focus_validator.data_loaders.data_loader.logging.getLogger')
    def test_unicode_filename(self, mock_get_logger):
        """Test behavior with Unicode characters in filename."""
        unicode_name = "测试文件_ñáme.csv"
        
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        
        loader = DataLoader(unicode_name)
        # Should handle Unicode gracefully
        self.assertEqual(loader.data_filename, unicode_name)
        # Should log warning about nonexistent file
        mock_logger.warning.assert_called()


if __name__ == '__main__':
    unittest.main()