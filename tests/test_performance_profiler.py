import cProfile
import csv
import io
import logging
import os
import pstats
import time
import unittest
from ddt import ddt, data, unpack

from tests.samples.csv_random_data_generate_at_scale import generate_and_write_fake_focuses
from focus_validator.validator import Validator

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')

@ddt
class TestPerformanceProfiler(unittest.TestCase):
    
    def profile_to_csv(self, profiling_result, csv_file):
        with open(csv_file, 'w', newline='') as f:
            w = csv.writer(f)
            # Write the headers
            headers = ['ncalls', 'tottime', 'percall', 'cumtime', 'percall', 'filename:lineno(function)']
            w.writerow(headers)
        
            # Write each row
            for row in profiling_result.stats.items():
                func_name, (cc, nc, tt, ct, callers) = row
                # Handle division by zero
                tt_per_call = tt/nc if nc > 0 else 0
                ct_per_call = ct/cc if cc > 0 else 0
                w.writerow([nc, tt, tt_per_call, ct, ct_per_call, func_name])

    def execute_profiler(self, file_name, performance_threshold):
        # Set the environment variable for logging level
        env = os.environ.copy()
        env["LOG_LEVEL"] = "INFO"

        # Get the current directory of this test file
        test_dir = os.path.dirname(os.path.abspath(__file__))
        base_dir =  os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        rule_set_path=os.path.join(base_dir, "focus_validator", "rules")
        validator = Validator(
            data_filename=os.path.join(test_dir, '../' + file_name),
            rule_set_path=rule_set_path,
            rules_version="1.2",
            output_type="console",
            output_destination=None,
            column_namespace=None,
            focus_dataset="CostAndUsage",
            rules_block_remote_download=True,
        )

        # Set up the profiler
        profiler = cProfile.Profile()
        profiler.enable()

        # The original performance testing code
        start_time = time.time()
        validator.validate()
        end_time = time.time()
        duration = end_time - start_time
        logging.info(f"File: {file_name} Duration: {duration} seconds")

        # Stop the profiler
        profiler.disable()

        # Save profiling data to a file
        profiling_result = pstats.Stats(profiler)
        profile_file_name = "profiling_data_" + file_name
        self.profile_to_csv(profiling_result, profile_file_name)

        # Optionally print out profiling report to the console
        s = io.StringIO()
        sortby = 'cumulative'  # Can be changed to 'time', 'calls', etc.
        ps = pstats.Stats(profiler, stream=s).sort_stats(sortby)
        ps.print_stats(10)
        logging.info(s.getvalue())

        #Execution time check
        self.assertLess(duration, performance_threshold, f"Performance test exceeded threshold. Duration: {duration} seconds")

    @data(
        # ("fake_focuses500000.csv", 60.0, 500000, "validate_500000_records"),
        # ("fake_focuses250000.csv", 60.0, 250000, "validate_250000_records"),
        # ("fake_focuses100000.csv", 30.0, 100000, "validate_100000_records"),
        # ("fake_focuses50000.csv", 15.0, 50000, "validate_50000_records"),
        # ("fake_focuses10000.csv", 7.0, 10000, "validate_10000_records"),
        # ("fake_focuses5000.csv", 3.0, 5000, "validate_5000_records"),
        ("fake_focuses2000.csv", 6.0, 2000, "validate_2000_records"),
        ("fake_focuses2000.csv", 6.0, 1000, "validate_1000_records")
    )
    @unpack
    def test_param_validator_performance(self, file_name, performance_threshold, number_of_records, case_id):
        with self.subTest(case_id=case_id):
            # Set the environment variable for logging level
            env = os.environ.copy()
            env["LOG_LEVEL"] = "INFO"

            logging.info("Generating file with {number_of_records} records.")
            generate_and_write_fake_focuses(file_name, number_of_records)
            self.execute_profiler(str(file_name), performance_threshold)

            logging.info("Cleaning up test file.")
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            if os.path.exists(os.path.join(base_dir, file_name)):
                os.remove(os.path.join(base_dir, file_name))
    
if __name__ == '__main__':
    unittest.main()