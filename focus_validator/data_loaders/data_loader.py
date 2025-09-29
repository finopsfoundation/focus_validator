import logging
import os
import time
from focus_validator.data_loaders.csv_data_loader import CSVDataLoader
from focus_validator.data_loaders.parquet_data_loader import ParquetDataLoader
from focus_validator.exceptions import FocusNotImplementedError
from focus_validator.utils.performance_logging import logPerformance


class DataLoader:
    def __init__(self, data_filename):
        self.log = logging.getLogger(f"{__name__}.{self.__class__.__qualname__}")
        self.data_filename = data_filename

        self.log.info("Initializing DataLoader for file: %s", data_filename)

        if data_filename and os.path.exists(data_filename):
            file_size = os.path.getsize(data_filename)
            self.log.info("File size: %.2f MB", file_size / 1024 / 1024)
        else:
            self.log.warning("Data file does not exist or path is None: %s", data_filename)

        self.data_loader_class = self.find_data_loader()
        self.data_loader = self.data_loader_class(self.data_filename)

    def find_data_loader(self):
        self.log.debug("Determining data loader for file: %s", self.data_filename)

        if self.data_filename.endswith(".csv"):
            self.log.debug("Using CSV data loader")
            return CSVDataLoader
        elif self.data_filename.endswith(".parquet"):
            self.log.debug("Using Parquet data loader")
            return ParquetDataLoader
        else:
            self.log.error("Unsupported file type: %s", self.data_filename)
            raise FocusNotImplementedError("File type not implemented yet.")

    @logPerformance("data_loader.load", includeArgs=True)
    def load(self):
        self.log.info("Loading data from file...")
        result = self.data_loader.load()

        if result is not None:
            try:
                row_count = len(result)
                col_count = len(result.columns) if hasattr(result, 'columns') else 'unknown'
                self.log.info("Data loaded successfully: %d rows, %s columns", row_count, col_count)

                if hasattr(result, 'columns'):
                    self.log.debug("Columns: %s", list(result.columns))
            except Exception as e:
                self.log.warning("Could not determine data dimensions: %s", e)
        else:
            self.log.warning("Data loading returned None")

        return result
