"""
Test cases for timezone handling in data loaders.

This module tests the timezone handling approach:
- Mixed timezones: All normalized to UTC (improved data quality)
- Single timezone: Converted to UTC for consistency  
- No timezone: Defaults to UTC

The loaders now successfully handle mixed timezone formats by normalizing
all datetime values to UTC, which provides better data quality than dropping
problematic columns.
"""

import unittest
import tempfile
import polars as pl
from focus_validator.data_loaders.csv_data_loader import CSVDataLoader
from focus_validator.data_loaders.parquet_data_loader import ParquetDataLoader
import os
import time


def safe_delete_file(filepath, max_retries=3):
    """Safely delete a file with retries for Windows compatibility."""
    for attempt in range(max_retries):
        try:
            if os.path.exists(filepath):
                os.unlink(filepath)
            return
        except PermissionError:
            if attempt < max_retries - 1:
                time.sleep(0.1)  # Brief delay before retry
            else:
                # Final attempt - if it still fails, ignore it
                # The temp file will be cleaned up by the OS eventually
                pass


class TestStrictTimezoneHandling(unittest.TestCase):
    """Test strict timezone handling for data quality."""

    def test_csv_mixed_timezones_normalized_to_utc(self):
        """Test that CSV columns with mixed timezones are normalized to UTC."""
        test_data = """Date,Value
2023-01-01T10:00:00-05:00,100
2023-01-02T15:30:00+02:00,200
2023-01-03T08:45:00Z,300"""
        
        # Create temp file
        fd, temp_path = tempfile.mkstemp(suffix='.csv', text=True)
        try:
            with os.fdopen(fd, 'w') as f:
                f.write(test_data)
            
            column_types = {'Date': pl.Datetime("us", "UTC")}
            loader = CSVDataLoader(temp_path, column_types=column_types)
            df = loader.load()
            
            # Column should be successfully converted and normalized to UTC
            self.assertIsNotNone(df)
            self.assertIn('Date', df.columns)
            self.assertNotIn('Date', loader.failed_columns)
            self.assertEqual(df['Date'].dtype.time_zone, 'UTC')
            self.assertIn('Value', df.columns)  # Other columns should remain
            
            # Verify the timestamps are correctly converted to UTC
            # 2023-01-01T10:00:00-05:00 -> 2023-01-01T15:00:00Z
            # 2023-01-02T15:30:00+02:00 -> 2023-01-02T13:30:00Z  
            # 2023-01-03T08:45:00Z -> 2023-01-03T08:45:00Z
            expected_utc_times = [
                '2023-01-01 15:00:00',  # -05:00 converted to UTC
                '2023-01-02 13:30:00',  # +02:00 converted to UTC
                '2023-01-03 08:45:00'   # Z already UTC
            ]
            actual_times = [str(dt).replace(' UTC', '').replace('+00:00', '') for dt in df['Date']]
            self.assertEqual(actual_times, expected_utc_times)
            
        finally:
            safe_delete_file(temp_path)

    def test_csv_single_timezone_converted_to_utc(self):
        """Test that CSV columns with single timezone are converted to UTC."""
        test_data = """Date,Value
2023-01-01T10:00:00-05:00,100
2023-01-02T15:30:00-05:00,200
2023-01-03T08:45:00-05:00,300"""
        
        # Create temp file
        fd, temp_path = tempfile.mkstemp(suffix='.csv', text=True)
        try:
            with os.fdopen(fd, 'w') as f:
                f.write(test_data)
            
            column_types = {'Date': pl.Datetime("us", "UTC")}
            loader = CSVDataLoader(temp_path, column_types=column_types)
            df = loader.load()
            
            # Column should be converted to UTC for consistency
            self.assertIsNotNone(df)
            self.assertIn('Date', df.columns)
            self.assertNotIn('Date', loader.failed_columns)
            self.assertEqual(df['Date'].dtype.time_zone, 'UTC')
            
            # Verify all timestamps are correctly converted to UTC
            # All -05:00 times should be +5 hours in UTC
            expected_utc_times = [
                '2023-01-01 15:00:00',  # 10:00 -05:00 -> 15:00 UTC
                '2023-01-02 20:30:00',  # 15:30 -05:00 -> 20:30 UTC
                '2023-01-03 13:45:00'   # 08:45 -05:00 -> 13:45 UTC
            ]
            actual_times = [str(dt).replace(' UTC', '').replace('+00:00', '') for dt in df['Date']]
            self.assertEqual(actual_times, expected_utc_times)
            
        finally:
            safe_delete_file(temp_path)

    def test_csv_no_timezone_defaults_to_utc(self):
        """Test that CSV columns without timezone default to UTC."""
        test_data = """Date,Value
2023-01-01 10:00:00,100
2023-01-02 15:30:00,200
2023-01-03 08:45:00,300"""
        
        # Create temp file
        fd, temp_path = tempfile.mkstemp(suffix='.csv', text=True)
        try:
            with os.fdopen(fd, 'w') as f:
                f.write(test_data)
            
            column_types = {'Date': pl.Datetime("us", "UTC")}
            loader = CSVDataLoader(temp_path, column_types=column_types)
            df = loader.load()
            
            # Column should default to UTC
            self.assertIsNotNone(df)
            self.assertIn('Date', df.columns)
            self.assertNotIn('Date', loader.failed_columns)
            # Check that timezone is UTC
            self.assertEqual(df['Date'].dtype.time_zone, 'UTC')
            
        finally:
            safe_delete_file(temp_path)

    def test_parquet_mixed_timezones_normalized_to_utc(self):
        """Test that Parquet columns with mixed timezones are normalized to UTC."""
        # Create test DataFrame with mixed timezones
        data = {
            'Date': ['2023-01-01T10:00:00-05:00', '2023-01-02T15:30:00+02:00', '2023-01-03T08:45:00Z'],
            'Value': [100, 200, 300]
        }
        df = pl.DataFrame(data)
        
        # Create temp file
        fd, temp_path = tempfile.mkstemp(suffix='.parquet')
        try:
            os.close(fd)  # Close the file descriptor
            df.write_parquet(temp_path)
            
            column_types = {'Date': pl.Datetime("us", "UTC")}
            loader = ParquetDataLoader(temp_path, column_types=column_types)
            result_df = loader.load()
            
            # Column should be successfully converted and normalized to UTC
            self.assertIsNotNone(result_df)
            self.assertIn('Date', result_df.columns)
            self.assertEqual(result_df['Date'].dtype.time_zone, 'UTC')
            self.assertIn('Value', result_df.columns)  # Other columns should remain
            
            # Verify the timestamps are correctly converted to UTC
            expected_utc_times = [
                '2023-01-01 15:00:00',  # -05:00 converted to UTC
                '2023-01-02 13:30:00',  # +02:00 converted to UTC
                '2023-01-03 08:45:00'   # Z already UTC
            ]
            actual_times = [str(dt).replace(' UTC', '').replace('+00:00', '') for dt in result_df['Date']]
            self.assertEqual(actual_times, expected_utc_times)
            
        finally:
            safe_delete_file(temp_path)

    def test_parquet_single_timezone_converted_to_utc(self):
        """Test that Parquet columns with single timezone are converted to UTC."""
        # Create test DataFrame with single timezone using Polars
        data = {
            'Date': ['2023-01-01T10:00:00-05:00', '2023-01-02T15:30:00-05:00', '2023-01-03T08:45:00-05:00'],
            'Value': [100, 200, 300]
        }
        df = pl.DataFrame(data)
        
        # Create temp file
        fd, temp_path = tempfile.mkstemp(suffix='.parquet')
        try:
            os.close(fd)  # Close the file descriptor
            df.write_parquet(temp_path)
            
            column_types = {'Date': pl.Datetime("us", "UTC")}
            loader = ParquetDataLoader(temp_path, column_types=column_types)
            result_df = loader.load()
            
            # Column should be converted to UTC for consistency
            self.assertIsNotNone(result_df)
            self.assertIn('Date', result_df.columns)
            self.assertEqual(result_df['Date'].dtype.time_zone, 'UTC')
            
            # Verify all timestamps are correctly converted to UTC
            # All -05:00 times should be +5 hours in UTC
            expected_utc_times = [
                '2023-01-01 15:00:00',  # 10:00 -05:00 -> 15:00 UTC
                '2023-01-02 20:30:00',  # 15:30 -05:00 -> 20:30 UTC
                '2023-01-03 13:45:00'   # 08:45 -05:00 -> 13:45 UTC
            ]
            actual_times = [str(dt).replace(' UTC', '').replace('+00:00', '') for dt in result_df['Date']]
            self.assertEqual(actual_times, expected_utc_times)
            
        finally:
            safe_delete_file(temp_path)

    def test_parquet_no_timezone_defaults_to_utc(self):
        """Test that Parquet columns without timezone default to UTC."""
        # Create test DataFrame without timezone using Polars
        data = {
            'Date': ['2023-01-01 10:00:00', '2023-01-02 15:30:00', '2023-01-03 08:45:00'],
            'Value': [100, 200, 300]
        }
        df = pl.DataFrame(data)
        
        # Create temp file
        fd, temp_path = tempfile.mkstemp(suffix='.parquet')
        try:
            os.close(fd)  # Close the file descriptor
            df.write_parquet(temp_path)
            
            column_types = {'Date': pl.Datetime("us", "UTC")}
            loader = ParquetDataLoader(temp_path, column_types=column_types)
            result_df = loader.load()
            
            # Column should default to UTC
            self.assertIsNotNone(result_df)
            self.assertIn('Date', result_df.columns)
            self.assertEqual(result_df['Date'].dtype.time_zone, 'UTC')
            
        finally:
            safe_delete_file(temp_path)

    def test_successful_mixed_timezone_handling(self):
        """Test that mixed timezone data is successfully normalized to UTC."""
        test_data = """Date,Value
2023-01-01T10:00:00-05:00,100
2023-01-02T15:30:00+02:00,200"""
        
        # Create temp file
        fd, temp_path = tempfile.mkstemp(suffix='.csv', text=True)
        try:
            with os.fdopen(fd, 'w') as f:
                f.write(test_data)
            
            column_types = {'Date': pl.Datetime("us", "UTC")}
            loader = CSVDataLoader(temp_path, column_types=column_types)
            
            # Should successfully handle mixed timezones
            df = loader.load()
            
            # Should not be in failed columns - successfully processed
            self.assertNotIn('Date', loader.failed_columns)
            self.assertIsNotNone(df)
            self.assertIn('Date', df.columns)
            self.assertEqual(df['Date'].dtype.time_zone, 'UTC')
            
            # Verify timezone conversion worked correctly
            expected_utc_times = [
                '2023-01-01 15:00:00',  # -05:00 converted to UTC
                '2023-01-02 13:30:00'   # +02:00 converted to UTC
            ]
            actual_times = [str(dt).replace(' UTC', '').replace('+00:00', '') for dt in df['Date']]
            self.assertEqual(actual_times, expected_utc_times)
            
        finally:
            safe_delete_file(temp_path)


if __name__ == '__main__':
    unittest.main()