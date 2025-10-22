"""
Test cases for strict timezone handling in data loaders.

This module tests the data quality-focused approach to timezone handling:
- Mixed timezones: Columns are dropped (data quality issue)
- Single timezone: Original timezone is preserved  
- No timezone: Defaults to UTC
"""

import unittest
import tempfile
import pandas as pd
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

    def test_csv_mixed_timezones_column_dropped(self):
        """Test that CSV columns with mixed timezones are dropped."""
        test_data = """Date,Value
2023-01-01T10:00:00-05:00,100
2023-01-02T15:30:00+02:00,200
2023-01-03T08:45:00Z,300"""
        
        # Create temp file
        fd, temp_path = tempfile.mkstemp(suffix='.csv', text=True)
        try:
            with os.fdopen(fd, 'w') as f:
                f.write(test_data)
            
            column_types = {'Date': 'datetime64[ns, UTC]'}
            loader = CSVDataLoader(temp_path, column_types=column_types)
            df = loader.load()
            
            # Column should be dropped due to mixed timezones
            self.assertIsNotNone(df)
            self.assertNotIn('Date', df.columns)
            self.assertIn('Date', loader.failed_columns)
            self.assertIn('Value', df.columns)  # Other columns should remain
            
        finally:
            safe_delete_file(temp_path)

    def test_csv_single_timezone_preserved(self):
        """Test that CSV columns with single timezone are preserved."""
        test_data = """Date,Value
2023-01-01T10:00:00-05:00,100
2023-01-02T15:30:00-05:00,200
2023-01-03T08:45:00-05:00,300"""
        
        # Create temp file
        fd, temp_path = tempfile.mkstemp(suffix='.csv', text=True)
        try:
            with os.fdopen(fd, 'w') as f:
                f.write(test_data)
            
            column_types = {'Date': 'datetime64[ns, UTC]'}
            loader = CSVDataLoader(temp_path, column_types=column_types)
            df = loader.load()
            
            # Column should be preserved with original timezone
            self.assertIsNotNone(df)
            self.assertIn('Date', df.columns)
            self.assertNotIn('Date', loader.failed_columns)
            self.assertEqual(str(df['Date'].dt.tz), 'UTC-05:00')
            
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
            
            column_types = {'Date': 'datetime64[ns, UTC]'}
            loader = CSVDataLoader(temp_path, column_types=column_types)
            df = loader.load()
            
            # Column should default to UTC
            self.assertIsNotNone(df)
            self.assertIn('Date', df.columns)
            self.assertNotIn('Date', loader.failed_columns)
            self.assertEqual(str(df['Date'].dt.tz), 'UTC')
            
        finally:
            safe_delete_file(temp_path)

    def test_parquet_mixed_timezones_column_dropped(self):
        """Test that Parquet columns with mixed timezones are dropped."""
        # Create test DataFrame with mixed timezones
        data = {
            'Date': ['2023-01-01T10:00:00-05:00', '2023-01-02T15:30:00+02:00', '2023-01-03T08:45:00Z'],
            'Value': [100, 200, 300]
        }
        df = pd.DataFrame(data)
        
        # Create temp file
        fd, temp_path = tempfile.mkstemp(suffix='.parquet')
        try:
            os.close(fd)  # Close the file descriptor
            df.to_parquet(temp_path)
            
            column_types = {'Date': 'datetime64[ns, UTC]'}
            loader = ParquetDataLoader(temp_path, column_types=column_types)
            result_df = loader.load()
            
            # Column should be dropped due to mixed timezones
            self.assertIsNotNone(result_df)
            self.assertNotIn('Date', result_df.columns)
            self.assertIn('Value', result_df.columns)  # Other columns should remain
            
        finally:
            safe_delete_file(temp_path)

    def test_parquet_single_timezone_preserved(self):
        """Test that Parquet columns with single timezone are preserved."""
        # Create test DataFrame with single timezone
        dates = pd.to_datetime(['2023-01-01 10:00:00', '2023-01-02 15:30:00', '2023-01-03 08:45:00'])
        dates = dates.tz_localize('US/Eastern')
        
        data = {
            'Date': dates,
            'Value': [100, 200, 300]
        }
        df = pd.DataFrame(data)
        
        # Create temp file
        fd, temp_path = tempfile.mkstemp(suffix='.parquet')
        try:
            os.close(fd)  # Close the file descriptor
            df.to_parquet(temp_path)
            
            column_types = {'Date': 'datetime64[ns, UTC]'}
            loader = ParquetDataLoader(temp_path, column_types=column_types)
            result_df = loader.load()
            
            # Column should be preserved with original timezone
            self.assertIsNotNone(result_df)
            self.assertIn('Date', result_df.columns)
            self.assertEqual(str(result_df['Date'].dt.tz), 'US/Eastern')
            
        finally:
            safe_delete_file(temp_path)

    def test_parquet_no_timezone_defaults_to_utc(self):
        """Test that Parquet columns without timezone default to UTC."""
        # Create test DataFrame without timezone
        dates = pd.to_datetime(['2023-01-01 10:00:00', '2023-01-02 15:30:00', '2023-01-03 08:45:00'])
        
        data = {
            'Date': dates,
            'Value': [100, 200, 300]
        }
        df = pd.DataFrame(data)
        
        # Create temp file
        fd, temp_path = tempfile.mkstemp(suffix='.parquet')
        try:
            os.close(fd)  # Close the file descriptor
            df.to_parquet(temp_path)
            
            column_types = {'Date': 'datetime64[ns, UTC]'}
            loader = ParquetDataLoader(temp_path, column_types=column_types)
            result_df = loader.load()
            
            # Column should default to UTC
            self.assertIsNotNone(result_df)
            self.assertIn('Date', result_df.columns)
            self.assertEqual(str(result_df['Date'].dt.tz), 'UTC')
            
        finally:
            safe_delete_file(temp_path)

    def test_data_quality_logging_for_mixed_timezones(self):
        """Test that appropriate logging occurs for mixed timezone data quality issues."""
        test_data = """Date,Value
2023-01-01T10:00:00-05:00,100
2023-01-02T15:30:00+02:00,200"""
        
        # Create temp file
        fd, temp_path = tempfile.mkstemp(suffix='.csv', text=True)
        try:
            with os.fdopen(fd, 'w') as f:
                f.write(test_data)
            
            column_types = {'Date': 'datetime64[ns, UTC]'}
            loader = CSVDataLoader(temp_path, column_types=column_types)
            
            # Capture log messages by checking failed columns
            df = loader.load()
            
            # Should have logged the data quality issue
            self.assertIn('Date', loader.failed_columns)
            self.assertIsNotNone(df)
            self.assertNotIn('Date', df.columns)
            
        finally:
            safe_delete_file(temp_path)


if __name__ == '__main__':
    unittest.main()