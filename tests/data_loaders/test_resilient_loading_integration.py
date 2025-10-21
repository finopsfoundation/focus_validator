"""Integration tests for resilient data loading with FOCUS rule-based column types."""

import unittest
from unittest.mock import Mock, patch, MagicMock
import tempfile
import os
import pandas as pd
import io
from datetime import datetime

from focus_validator.data_loaders.csv_data_loader import CSVDataLoader
from focus_validator.data_loaders.parquet_data_loader import ParquetDataLoader
from focus_validator.data_loaders.data_loader import DataLoader


class TestResilientLoadingIntegration(unittest.TestCase):
    """Test integration of resilient loading with extracted column types from FOCUS rules."""

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

    def test_csv_loader_with_focus_extracted_column_types(self):
        """Test CSV loader using column types extracted from FOCUS rules."""
        # Create CSV with FOCUS-like data containing problematic values
        self.temp_csv = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        self.temp_csv.write("BilledCost,BillingPeriodStart,AvailabilityZone,Tags\n")
        self.temp_csv.write("123.45,2023-01-01T00:00:00Z,us-east-1a,key1:value1\n")
        self.temp_csv.write("INVALID_COST,BAD_DATE,eu-west-1b,key2:value2\n")
        self.temp_csv.write("67.89,2023-03-01T00:00:00Z,,key3:value3\n")
        self.temp_csv.close()
        
        # Simulate column types extracted from FOCUS rules
        extracted_column_types = {
            'BilledCost': 'float64',  # TypeDecimal from Function "Type"
            'BillingPeriodStart': 'datetime64[ns, UTC]',  # TypeDateTime from Function "Type"
            'AvailabilityZone': 'string',  # TypeString from Function "Type"
            'Tags': 'string'  # TypeString from Function "Type"
        }
        
        loader = CSVDataLoader(self.temp_csv.name, column_types=extracted_column_types)
        result = loader.load()
        
        # Should successfully load with proper type conversions and coercion
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 3)
        
        # Check type conversions worked
        self.assertEqual(result['BilledCost'].dtype, 'float64')
        self.assertEqual(str(result['BillingPeriodStart'].dtype), 'datetime64[ns, UTC]')
        self.assertEqual(result['AvailabilityZone'].dtype.name, 'string')
        self.assertEqual(result['Tags'].dtype.name, 'string')
        
        # Check that invalid values were coerced appropriately
        self.assertTrue(pd.isna(result['BilledCost'].iloc[1]))  # INVALID_COST -> NaN
        self.assertTrue(pd.isna(result['BillingPeriodStart'].iloc[1]))  # BAD_DATE -> NaT
        
        # Check valid values preserved
        self.assertEqual(result['BilledCost'].iloc[0], 123.45)
        self.assertEqual(result['BilledCost'].iloc[2], 67.89)

    def test_parquet_loader_with_focus_extracted_column_types(self):
        """Test Parquet loader using column types extracted from FOCUS rules."""
        # Create Parquet with FOCUS-like data
        test_data = pd.DataFrame({
            'BilledCost': ['123.45', 'INVALID', '67.89'],  # String data to be converted
            'BillingPeriodStart': ['2023-01-01T00:00:00Z', 'BAD_DATE', '2023-03-01T00:00:00Z'],
            'AvailabilityZone': ['us-east-1a', 'eu-west-1b', 'ap-south-1c'],
            'ServiceCategory': ['Compute', 'Storage', 'Network']
        })
        
        self.temp_parquet = tempfile.NamedTemporaryFile(suffix='.parquet', delete=False)
        self.temp_parquet.close()
        test_data.to_parquet(self.temp_parquet.name, index=False)
        
        # Simulate column types extracted from FOCUS rules
        extracted_column_types = {
            'BilledCost': 'float64',  # TypeDecimal from Function "Type"
            'BillingPeriodStart': 'datetime64[ns, UTC]',  # TypeDateTime from Function "Type"
            'AvailabilityZone': 'string',  # TypeString from Function "Type"
            'ServiceCategory': 'string'  # TypeString from Function "Type"
        }
        
        loader = ParquetDataLoader(self.temp_parquet.name, column_types=extracted_column_types)
        result = loader.load()
        
        # Should successfully load with proper type conversions and coercion
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 3)
        
        # Check type conversions worked
        self.assertEqual(result['BilledCost'].dtype, 'float64')
        self.assertEqual(str(result['BillingPeriodStart'].dtype), 'datetime64[ns, UTC]')
        self.assertEqual(result['AvailabilityZone'].dtype.name, 'string')
        self.assertEqual(result['ServiceCategory'].dtype.name, 'string')
        
        # Check coercion results
        self.assertEqual(result['BilledCost'].iloc[0], 123.45)
        self.assertTrue(pd.isna(result['BilledCost'].iloc[1]))  # INVALID -> NaN
        self.assertEqual(result['BilledCost'].iloc[2], 67.89)

    def test_data_loader_integration_with_column_types(self):
        """Test DataLoader class integration with column types parameter."""
        # Create CSV for testing DataLoader integration
        self.temp_csv = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        self.temp_csv.write("ResourceId,BilledCost,BillingPeriodStart\n")
        self.temp_csv.write("resource1,123.45,2023-01-01T00:00:00Z\n")
        self.temp_csv.write("resource2,INVALID,BAD_DATE\n")
        self.temp_csv.close()
        
        # Mock column types that would be extracted from FOCUS rules
        extracted_column_types = {
            'ResourceId': 'string',
            'BilledCost': 'float64',
            'BillingPeriodStart': 'datetime64[ns, UTC]'
        }
        
        # Test that DataLoader can be initialized with column types
        # Note: Current DataLoader doesn't accept column_types, so we test the underlying loader
        csv_loader = CSVDataLoader(self.temp_csv.name, column_types=extracted_column_types)
        result = csv_loader.load()
        
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 2)
        
        # Verify types applied correctly
        self.assertEqual(result['ResourceId'].dtype.name, 'string')
        self.assertEqual(result['BilledCost'].dtype, 'float64')
        self.assertEqual(str(result['BillingPeriodStart'].dtype), 'datetime64[ns, UTC]')

    def test_comprehensive_focus_column_type_mapping(self):
        """Test comprehensive mapping of FOCUS column types to pandas types."""
        # Create test data covering various FOCUS column types
        self.temp_csv = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        self.temp_csv.write("ResourceId,BilledCost,BillingPeriodStart,CommitmentDiscountQuantity,PricingQuantity\n")
        self.temp_csv.write("resource1,123.45,2023-01-01T00:00:00Z,100,5\n")
        self.temp_csv.write("resource2,INVALID,BAD_DATE,INVALID_DECIMAL,INVALID_INT\n")
        self.temp_csv.write("resource3,67.89,2023-03-01T00:00:00Z,200.5,10\n")
        self.temp_csv.close()
        
        # Comprehensive column type mapping from FOCUS Function "Type" entities
        focus_column_types = {
            # TypeString -> string
            'ResourceId': 'string',
            # TypeDecimal -> float64  
            'BilledCost': 'float64',
            'CommitmentDiscountQuantity': 'float64',
            # TypeDateTime -> datetime64[ns, UTC]
            'BillingPeriodStart': 'datetime64[ns, UTC]',
            # TypeInteger -> int64 (if such type exists in FOCUS)
            'PricingQuantity': 'int64'
        }
        
        loader = CSVDataLoader(self.temp_csv.name, column_types=focus_column_types)
        result = loader.load()
        
        # Verify all type conversions worked
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 3)
        
        # Check all expected types
        self.assertEqual(result['ResourceId'].dtype.name, 'string')
        self.assertEqual(result['BilledCost'].dtype, 'float64')
        self.assertEqual(result['CommitmentDiscountQuantity'].dtype, 'float64')
        self.assertEqual(str(result['BillingPeriodStart'].dtype), 'datetime64[ns, UTC]')
        self.assertEqual(str(result['PricingQuantity'].dtype), 'Int64')  # Nullable int for NaN handling
        
        # Check coercion of invalid values
        self.assertTrue(pd.isna(result['BilledCost'].iloc[1]))
        self.assertTrue(pd.isna(result['CommitmentDiscountQuantity'].iloc[1]))
        self.assertTrue(pd.isna(result['BillingPeriodStart'].iloc[1]))
        self.assertTrue(pd.isna(result['PricingQuantity'].iloc[1]))

    def test_partial_column_type_extraction_scenario(self):
        """Test scenario where only some columns have types extracted from FOCUS rules."""
        # Create CSV with mix of FOCUS columns and non-FOCUS columns
        self.temp_csv = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        self.temp_csv.write("BilledCost,CustomColumn1,AvailabilityZone,CustomColumn2\n")
        self.temp_csv.write("123.45,custom1,us-east-1a,999\n")
        self.temp_csv.write("67.89,custom2,eu-west-1b,888\n")
        self.temp_csv.close()
        
        # Only some columns have types from FOCUS rules
        partial_column_types = {
            'BilledCost': 'float64',  # From FOCUS TypeDecimal
            'AvailabilityZone': 'string'  # From FOCUS TypeString
            # CustomColumn1 and CustomColumn2 not in FOCUS, so no type specified
        }
        
        loader = CSVDataLoader(self.temp_csv.name, column_types=partial_column_types)
        result = loader.load()
        
        # Should work with partial column type specification
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 2)
        
        # FOCUS columns should have specified types
        self.assertEqual(result['BilledCost'].dtype, 'float64')
        self.assertEqual(result['AvailabilityZone'].dtype.name, 'string')
        
        # Non-FOCUS columns should keep pandas default inference
        # (exact type depends on pandas inference, but should not be the specified types)
        self.assertNotEqual(result['CustomColumn1'].dtype.name, 'string')
        self.assertNotEqual(result['CustomColumn2'].dtype, 'float64')

    def test_error_recovery_with_problematic_columns(self):
        """Test error recovery when some columns fail type conversion completely."""
        # Create data with extremely problematic columns
        self.temp_csv = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        self.temp_csv.write("GoodColumn,ProblematicColumn,AnotherGoodColumn\n")
        self.temp_csv.write("123,text_data,2023-01-01\n")
        self.temp_csv.write("456,more_text,2023-02-01\n")
        self.temp_csv.close()
        
        column_types = {
            'GoodColumn': 'float64',
            'ProblematicColumn': 'datetime64[ns, UTC]',  # Will fail - can't convert text to datetime
            'AnotherGoodColumn': 'datetime64[ns, UTC]'
        }
        
        loader = CSVDataLoader(self.temp_csv.name, column_types=column_types)
        result = loader.load()
        
        # Should still load successfully despite problematic column
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 2)
        
        # Good columns should be converted
        self.assertEqual(result['GoodColumn'].dtype, 'float64')
        self.assertEqual(str(result['AnotherGoodColumn'].dtype), 'datetime64[ns, UTC]')
        
        # Problematic column should be converted to datetime with NaT values for invalid data
        self.assertEqual(str(result['ProblematicColumn'].dtype), 'datetime64[ns, UTC]')
        self.assertTrue(result['ProblematicColumn'].isna().all())  # All values should be NaT
        
        # Failed columns should be empty since coercion handles all problems gracefully
        self.assertEqual(len(loader.failed_columns), 0)

    def test_datetime_timezone_handling(self):
        """Test proper handling of datetime columns with timezone specifications."""
        # Create CSV with various datetime formats
        self.temp_csv = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        self.temp_csv.write("UTCDateTime,LocalDateTime,BadDateTime\n")
        self.temp_csv.write("2023-01-01T00:00:00Z,2023-01-01 10:00:00,INVALID\n")
        self.temp_csv.write("2023-02-01T12:30:45Z,2023-02-01 15:30:45,BAD_DATE\n")
        self.temp_csv.close()
        
        column_types = {
            'UTCDateTime': 'datetime64[ns, UTC]',  # With timezone
            'LocalDateTime': 'datetime64[ns]',  # Without timezone
            'BadDateTime': 'datetime64[ns, UTC]'  # Will have invalid values
        }
        
        loader = CSVDataLoader(self.temp_csv.name, column_types=column_types)
        result = loader.load()
        
        # Should handle timezone specifications correctly
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 2)
        
        # Check timezone handling
        self.assertEqual(str(result['UTCDateTime'].dtype), 'datetime64[ns, UTC]')
        self.assertTrue(str(result['LocalDateTime'].dtype).startswith('datetime64'))
        self.assertEqual(str(result['BadDateTime'].dtype), 'datetime64[ns, UTC]')
        
        # Check that invalid dates were coerced to NaT
        self.assertTrue(pd.isna(result['BadDateTime'].iloc[0]))
        self.assertTrue(pd.isna(result['BadDateTime'].iloc[1]))

    def test_large_dataset_resilient_loading(self):
        """Test resilient loading with larger dataset containing scattered errors."""
        # Create larger CSV with scattered data quality issues
        self.temp_csv = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        self.temp_csv.write("BilledCost,BillingPeriodStart,AvailabilityZone\n")
        
        # Write 100 rows with 10% bad data
        for i in range(100):
            if i % 10 == 3:  # Every 10th row starting at 3 has bad cost
                cost = "INVALID"
            else:
                cost = f"{100 + i}.{i:02d}"
                
            if i % 10 == 7:  # Every 10th row starting at 7 has bad date
                date = "BAD_DATE"
            else:
                date = f"2023-{(i % 12) + 1:02d}-01T00:00:00Z"
                
            zone = f"region-{i % 5}"
            self.temp_csv.write(f"{cost},{date},{zone}\n")
        
        self.temp_csv.close()
        
        column_types = {
            'BilledCost': 'float64',
            'BillingPeriodStart': 'datetime64[ns, UTC]',
            'AvailabilityZone': 'string'
        }
        
        loader = CSVDataLoader(self.temp_csv.name, column_types=column_types)
        result = loader.load()
        
        # Should successfully load all 100 rows
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 100)
        
        # Check that types were applied correctly
        self.assertEqual(result['BilledCost'].dtype, 'float64')
        self.assertEqual(str(result['BillingPeriodStart'].dtype), 'datetime64[ns, UTC]')
        self.assertEqual(result['AvailabilityZone'].dtype.name, 'string')
        
        # Check that bad data was coerced appropriately
        # Should have exactly 10 NaN values in BilledCost (rows 3, 13, 23, ...)
        nan_count = result['BilledCost'].isna().sum()
        self.assertEqual(nan_count, 10)
        
        # Should have exactly 10 NaT values in BillingPeriodStart (rows 7, 17, 27, ...)
        nat_count = result['BillingPeriodStart'].isna().sum()
        self.assertEqual(nat_count, 10)

    def test_mixed_file_types_consistency(self):
        """Test that CSV and Parquet loaders handle column types consistently."""
        # Create identical test data for both CSV and Parquet
        test_data = pd.DataFrame({
            'BilledCost': ['123.45', 'INVALID', '67.89'],
            'BillingPeriodStart': ['2023-01-01T00:00:00Z', 'BAD_DATE', '2023-03-01T00:00:00Z'],
            'AvailabilityZone': ['us-east-1a', 'eu-west-1b', 'ap-south-1c']
        })
        
        # Save as CSV
        self.temp_csv = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        self.temp_csv.close()
        test_data.to_csv(self.temp_csv.name, index=False)
        
        # Save as Parquet
        self.temp_parquet = tempfile.NamedTemporaryFile(suffix='.parquet', delete=False)
        self.temp_parquet.close()
        test_data.to_parquet(self.temp_parquet.name, index=False)
        
        column_types = {
            'BilledCost': 'float64',
            'BillingPeriodStart': 'datetime64[ns, UTC]',
            'AvailabilityZone': 'string'
        }
        
        # Load with both loaders
        csv_loader = CSVDataLoader(self.temp_csv.name, column_types=column_types)
        parquet_loader = ParquetDataLoader(self.temp_parquet.name, column_types=column_types)
        
        csv_result = csv_loader.load()
        parquet_result = parquet_loader.load()
        
        # Results should be consistent between file types
        self.assertEqual(csv_result.shape, parquet_result.shape)
        self.assertEqual(list(csv_result.columns), list(parquet_result.columns))
        
        # Types should be consistent
        self.assertEqual(csv_result['BilledCost'].dtype, parquet_result['BilledCost'].dtype)
        self.assertEqual(str(csv_result['BillingPeriodStart'].dtype), str(parquet_result['BillingPeriodStart'].dtype))
        self.assertEqual(csv_result['AvailabilityZone'].dtype.name, parquet_result['AvailabilityZone'].dtype.name)
        
        # Coercion results should be consistent
        self.assertEqual(csv_result['BilledCost'].isna().sum(), parquet_result['BilledCost'].isna().sum())
        self.assertEqual(csv_result['BillingPeriodStart'].isna().sum(), parquet_result['BillingPeriodStart'].isna().sum())


if __name__ == '__main__':
    unittest.main()