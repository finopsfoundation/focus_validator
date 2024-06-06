from focus_validator.data_loaders.csv_data_loader import CSVDataLoader
from focus_validator.data_loaders.parquet_data_loader import ParquetDataLoader
from focus_validator.exceptions import FocusNotImplementedError


class DataLoader:
    def __init__(self, data_filename):
        self.data_filename = data_filename
        self.data_loader_class = self.find_data_loader()
        self.data_loader = self.data_loader_class(self.data_filename)

    def find_data_loader(self):
        if self.data_filename.endswith(".csv"):
            return CSVDataLoader
        elif self.data_filename.endswith(".parquet"):
            return ParquetDataLoader
        else:
            raise FocusNotImplementedError("File type not implemented yet.")

    def load(self):
        return self.data_loader.load()
