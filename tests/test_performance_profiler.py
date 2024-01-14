import cProfile
import csv
import io
import logging
import os
import pstats
import time
import unittest
from ddt import ddt, data, unpack
from focus_validator.utils.profiler import Profiler

from tests.samples.csv_random_data_generate_at_scale import generate_and_write_fake_focuses
from focus_validator.validator import Validator

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')

@ddt
class TestPerformanceProfiler(unittest.TestCase):
    
    def measure_validator(self, file_name, performance_threshold):
        # Set the environment variable for logging level
        env = os.environ.copy()
        env["LOG_LEVEL"] = "INFO"

        # Get the current directory of this test file
        test_dir = os.path.dirname(os.path.abspath(__file__))
        base_dir =  os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        version_set_path=os.path.join(base_dir, "focus_validator", "rules", "version_sets")
        validator = Validator(
            data_filename=os.path.join(test_dir, '../' + file_name),
            override_filename=None,
            rule_set_path=version_set_path,
            rules_version="0.5",
            output_type="console",
            output_destination=None,
            column_namespace=None,
        )

        # The measure execution
        start_time = time.time()
        self.run_and_profile_validator(validator)
        end_time = time.time()
        duration = end_time - start_time
        logging.info(f"File: {file_name} Duration: {duration} seconds")

        #Execution time check
        self.assertLess(duration, performance_threshold, f"Performance test exceeded threshold. Duration: {duration} seconds")

    @Profiler(csv_format=True)
    def run_and_profile_validator(self, validator):
        validator.validate()

    @data(
        # ("fake_focuses500000.csv", 110.0, 500000, "validate_500000_records"),
        # ("fake_focuses250000.csv", 60.0, 250000, "validate_250000_records"),
        # ("fake_focuses100000.csv", 20.0, 100000, "validate_100000_records"),
        # ("fake_focuses50000.csv", 11.0, 50000, "validate_50000_records"),
        # ("fake_focuses10000.csv", 2.5, 10000, "validate_10000_records"),
        # ("fake_focuses5000.csv", 1.8, 5000, "validate_5000_records"),
        ("fake_focuses2000.csv", 1.0, 2000, "validate_2000_records"),
        ("fake_focuses2000.csv", 1.0, 1000, "validate_1000_records")
    )
    @unpack
    def test_param_validator_performance(self, file_name, performance_threshold, number_of_records, case_id):
        with self.subTest(case_id=case_id):
            # Set the environment variable for logging level
            env = os.environ.copy()
            env["LOG_LEVEL"] = "INFO"

            logging.info(f"Generating file with {number_of_records} records.")
            generate_and_write_fake_focuses(file_name, number_of_records)
            self.measure_validator(str(file_name), performance_threshold)

            logging.info("Cleaning up test file.")
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            if os.path.exists(os.path.join(base_dir, file_name)):
                os.remove(os.path.join(base_dir, file_name))
    
if __name__ == '__main__':
    unittest.main()