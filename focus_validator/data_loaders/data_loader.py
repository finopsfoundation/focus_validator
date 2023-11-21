import magic

from focus_validator.data_loaders.csv_data_loader import CSVDataLoader
from focus_validator.data_loaders.parquet_data_loader import ParquetDataLoader
from focus_validator.exceptions import FocusNotImplementedError


def get_file_mime_type(filename):
    f = magic.Magic(uncompress=True)
    return f.from_file(filename=filename)


class DataLoader:
    def __init__(self, data_filename):
        self.data_filename = data_filename
        self.data_loader_class = self.find_data_loader()
        self.data_loader = self.data_loader_class(self.data_filename)

    def find_data_loader(self):
        file_mime_type = get_file_mime_type(self.data_filename)

        if file_mime_type in ["ASCII text", "CSV text", "CSV ASCII text"]:
            return CSVDataLoader
        elif file_mime_type == "Apache Parquet":
            return ParquetDataLoader
        else:
            raise FocusNotImplementedError(
                msg=f"Validator for file_type '{file_mime_type}' not implemented yet."
            )

    def load(self):
        return self.data_loader.load()
