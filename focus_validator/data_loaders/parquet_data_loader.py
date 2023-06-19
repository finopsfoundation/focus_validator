import pandas as pd


class ParquetDataLoader:
    def __init__(self, data_filename):
        self.data_filename = data_filename

    def load(self):
        return pd.read_parquet(self.data_filename)
