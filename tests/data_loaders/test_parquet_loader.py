from unittest import TestCase

import pandas as pd

from focus_validator.data_loaders.data_loader import DataLoader
from focus_validator.data_loaders.parquet_data_loader import ParquetDataLoader


class TestParquetLoader(TestCase):
    def test_load_parquet_file(self):
        data_loader = DataLoader(data_filename="samples/sample.parquet")
        df = data_loader.load()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(
            list(df.columns), ["InvoiceIssuer", "ResourceID", "ChargeType"]
        )

    def test_find_data_loader(self):
        data_loader = DataLoader(data_filename="samples/sample.parquet")
        data_loader_class = data_loader.find_data_loader()
        self.assertEqual(data_loader_class, ParquetDataLoader)
