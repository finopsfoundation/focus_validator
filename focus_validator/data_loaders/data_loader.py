import magic

from focus_validator.exceptions import FocusNotImplementedError
from .csv_data_loader import CSVDataLoader


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

        if file_mime_type in ["ASCII text", "CSV text"]:
            return CSVDataLoader
        elif file_mime_type == "Apache Parquet":
            raise FocusNotImplementedError(msg="Parquet read not implemented.")
        else:
            raise FocusNotImplementedError(
                msg=f"Validator for file_type {file_mime_type} not implemented yet."
            )

    def load(self):
        return self.data_loader.load()
