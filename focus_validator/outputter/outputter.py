from focus_validator.outputter.outputter_console import ConsoleOutputter

class Outputter:
    def __init__(self, output_destination):
        self.output_destination = output_destination
        self.console_outputter = ConsoleOutputter(output_destination=output_destination)
        # TODO: map to the correct outputter based on output
        self.outputter = self.console_outputter
    
    def write(self, result_set):
        self.outputter.write(result_set)