from focus_validator.data_loaders.csv_data_loader import CSVDataLoader

class DataLoader:
    def __init__(self, data_filename):
        self.data_filename = data_filename
        self.data_loader_class = self.find_data_loader()
        self.data_loader = self.data_loader_class(self.data_filename)

    def find_data_loader(self):
        # TODO: We should make this smarter than just extension checking
        if self.data_filename.endswith('.csv'):
            return CSVDataLoader
        else:
            # TODO: Probably should raise an error here about being unable to load this type of file input
            return None
    
    def load(self):
        return self.data_loader.load()
