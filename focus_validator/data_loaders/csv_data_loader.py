import pandas as pd


class CSVDataLoader:
    def __init__(self, data_filename):
        self.data_filename = data_filename

    def load(self):
        return pd.read_csv(self.data_filename, keep_default_na=False)
