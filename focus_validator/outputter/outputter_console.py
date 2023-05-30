import tabulate
import pandas as pd


class ConsoleOutputter:
    def __init__(self, output_destination):
        self.output_destination = output_destination
        self.result_set = None

    def process_result_set(self):
        # TODO: Take in a result object and turn it into a pd for tablulation
        return self.result_set

    def write(self, result_set):
        self.result_set = result_set
        self.formatted_result = self.process_result_set()
        print(result_set)
        #print(tabulate(self.result_set, headers='keys', tablefmt='psql'))
