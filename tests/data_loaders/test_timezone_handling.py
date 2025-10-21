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


class TestStrictTimezoneHandling(unittest.TestCase):
    """Test strict timezone handling for data quality."""

    def test_csv_mixed_timezones_column_dropped(self):
        """Test that CSV columns with mixed timezones are dropped."""
        test_data = """Date,Value
2023-01-01T10:00:00-05:00,100
2023-01-02T15:30:00+02:00,200
2023-01-03T08:45:00Z,300"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(test_data)
            f.flush()
            
            try:
                column_types = {'Date': 'datetime64[ns, UTC]'}
                loader = CSVDataLoader(f.name, column_types=column_types)
                df = loader.load()
                
                # Column should be dropped due to mixed timezones
                self.assertIsNotNone(df)
                self.assertNotIn('Date', df.columns)
                self.assertIn('Date', loader.failed_columns)
                self.assertIn('Value', df.columns)  # Other columns should remain
                
            finally:
                os.unlink(f.name)

    def test_csv_single_timezone_preserved(self):
        """Test that CSV columns with single timezone are preserved."""
        test_data = """Date,Value
2023-01-01T10:00:00-05:00,100
2023-01-02T15:30:00-05:00,200
2023-01-03T08:45:00-05:00,300"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(test_data)
            f.flush()
            
            try:
                column_types = {'Date': 'datetime64[ns, UTC]'}
                loader = CSVDataLoader(f.name, column_types=column_types)
                df = loader.load()
                
                # Column should be preserved with original timezone
                self.assertIsNotNone(df)
                self.assertIn('Date', df.columns)
                self.assertNotIn('Date', loader.failed_columns)
                self.assertEqual(str(df['Date'].dt.tz), 'UTC-05:00')
                
            finally:
                os.unlink(f.name)

    def test_csv_no_timezone_defaults_to_utc(self):
        """Test that CSV columns without timezone default to UTC."""
        test_data = """Date,Value
2023-01-01 10:00:00,100
2023-01-02 15:30:00,200
2023-01-03 08:45:00,300"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(test_data)
            f.flush()
            
            try:
                column_types = {'Date': 'datetime64[ns, UTC]'}
                loader = CSVDataLoader(f.name, column_types=column_types)
                df = loader.load()
                
                # Column should default to UTC
                self.assertIsNotNone(df)
                self.assertIn('Date', df.columns)
                self.assertNotIn('Date', loader.failed_columns)
                self.assertEqual(str(df['Date'].dt.tz), 'UTC')
                
            finally:
                os.unlink(f.name)

    def test_parquet_mixed_timezones_column_dropped(self):
        """Test that Parquet columns with mixed timezones are dropped."""
        # Create test DataFrame with mixed timezones
        data = {
            'Date': ['2023-01-01T10:00:00-05:00', '2023-01-02T15:30:00+02:00', '2023-01-03T08:45:00Z'],
            'Value': [100, 200, 300]
        }
        df = pd.DataFrame(data)
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            df.to_parquet(f.name)
            
            try:
                column_types = {'Date': 'datetime64[ns, UTC]'}
                loader = ParquetDataLoader(f.name, column_types=column_types)
                result_df = loader.load()
                
                # Column should be dropped due to mixed timezones
                self.assertIsNotNone(result_df)
                self.assertNotIn('Date', result_df.columns)
                self.assertIn('Value', result_df.columns)  # Other columns should remain
                
            finally:
                os.unlink(f.name)

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
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            df.to_parquet(f.name)
            
            try:
                column_types = {'Date': 'datetime64[ns, UTC]'}
                loader = ParquetDataLoader(f.name, column_types=column_types)
                result_df = loader.load()
                
                # Column should be preserved with original timezone
                self.assertIsNotNone(result_df)
                self.assertIn('Date', result_df.columns)
                self.assertEqual(str(result_df['Date'].dt.tz), 'US/Eastern')
                
            finally:
                os.unlink(f.name)

    def test_parquet_no_timezone_defaults_to_utc(self):
        """Test that Parquet columns without timezone default to UTC."""
        # Create test DataFrame without timezone
        dates = pd.to_datetime(['2023-01-01 10:00:00', '2023-01-02 15:30:00', '2023-01-03 08:45:00'])
        
        data = {
            'Date': dates,
            'Value': [100, 200, 300]
        }
        df = pd.DataFrame(data)
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            df.to_parquet(f.name)
            
            try:
                column_types = {'Date': 'datetime64[ns, UTC]'}
                loader = ParquetDataLoader(f.name, column_types=column_types)
                result_df = loader.load()
                
                # Column should default to UTC
                self.assertIsNotNone(result_df)
                self.assertIn('Date', result_df.columns)
                self.assertEqual(str(result_df['Date'].dt.tz), 'UTC')
                
            finally:
                os.unlink(f.name)

    def test_data_quality_logging_for_mixed_timezones(self):
        """Test that appropriate logging occurs for mixed timezone data quality issues."""
        test_data = """Date,Value
2023-01-01T10:00:00-05:00,100
2023-01-02T15:30:00+02:00,200"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(test_data)
            f.flush()
            
            try:
                column_types = {'Date': 'datetime64[ns, UTC]'}
                loader = CSVDataLoader(f.name, column_types=column_types)
                
                # Capture log messages by checking failed columns
                df = loader.load()
                
                # Should have logged the data quality issue
                self.assertIn('Date', loader.failed_columns)
                self.assertIsNotNone(df)
                self.assertNotIn('Date', df.columns)
                
            finally:
                os.unlink(f.name)


if __name__ == '__main__':
    unittest.main()