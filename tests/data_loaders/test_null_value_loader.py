import io
from unittest import TestCase

import pandas as pd

from focus_validator.data_loaders.csv_data_loader import CSVDataLoader
from focus_validator.data_loaders.parquet_data_loader import ParquetDataLoader


class TestNullValueLoader(TestCase):
    def test_null_value_from_csv(self):
        sample_data = pd.DataFrame([{"value": "NULL"}])

        buffer = io.BytesIO()
        sample_data.to_csv(buffer, index=False, lineterminator="\n")

        buffer.seek(0)
        self.assertEqual(buffer.read(), b"value\nNULL\n")

        buffer.seek(0)
        loader = CSVDataLoader(buffer)
        data = loader.load()

        self.assertTrue(pd.isnull(data.to_dict(orient="records")[0]["value"]))

    def test_null_value_from_csv_with_missing_value(self):
        sample_data = pd.DataFrame([{"value": None}])

        buffer = io.BytesIO()
        sample_data.to_csv(buffer, index=False, lineterminator="\n")

        buffer.seek(0)
        self.assertEqual(buffer.read(), b'value\n""\n')

        buffer.seek(0)
        loader = CSVDataLoader(buffer)
        data = loader.load()

        self.assertTrue(pd.isnull(data.to_dict(orient="records")[0]["value"]))

    def test_null_value_from_parquet(self):
        sample_data = pd.DataFrame([{"value": "NULL"}])

        buffer = io.BytesIO()
        sample_data.to_parquet(buffer, index=False)

        buffer.seek(0)
        loader = ParquetDataLoader(buffer)
        data = loader.load()

        self.assertEqual(data.to_dict(orient="records")[0], {"value": "NULL"})
