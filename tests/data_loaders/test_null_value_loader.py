import io
from unittest import TestCase

import polars as pl

from focus_validator.data_loaders.csv_data_loader import CSVDataLoader
from focus_validator.data_loaders.parquet_data_loader import ParquetDataLoader


class TestNullValueLoader(TestCase):
    def test_null_value_from_csv(self):
        sample_data = pl.DataFrame([{"value": "NULL"}])

        buffer = io.BytesIO()
        sample_data.write_csv(buffer)

        buffer.seek(0)
        self.assertEqual(buffer.read(), b"value\nNULL\n")

        buffer.seek(0)
        loader = CSVDataLoader(buffer)
        data = loader.load()

        # Check that NULL string was converted to actual null/None
        self.assertTrue(data["value"][0] is None)

    def test_null_value_from_csv_with_missing_value(self):
        sample_data = pl.DataFrame([{"value": None}])

        buffer = io.BytesIO()
        sample_data.write_csv(buffer)

        buffer.seek(0)
        self.assertEqual(buffer.read(), b'value\n\n')  # Polars writes empty string for None

        buffer.seek(0)
        loader = CSVDataLoader(buffer)
        data = loader.load()

        # Check that empty value was treated as null
        self.assertTrue(data["value"][0] is None)

    def test_null_value_from_parquet(self):
        sample_data = pl.DataFrame([{"value": "NULL"}])

        buffer = io.BytesIO()
        sample_data.write_parquet(buffer)

        buffer.seek(0)
        loader = ParquetDataLoader(buffer)
        data = loader.load()

        # In Parquet, "NULL" string stays as string (not converted to null)
        self.assertEqual(data["value"][0], "NULL")
