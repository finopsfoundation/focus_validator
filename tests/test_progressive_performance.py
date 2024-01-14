import logging
import os
import subprocess
import time
import unittest
from ddt import ddt, data, unpack
from focus_validator.utils.profiler import Profiler
from tests.samples.csv_random_data_generate_at_scale import generate_and_write_fake_focuses

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')

@ddt
class TestProgressivePerformance(unittest.TestCase):
    
    @Profiler(csv_format=True)
    def run_validator(self, args):
        # Get the current directory of this test file
        test_dir = os.path.dirname(os.path.abspath(__file__))

        # Construct the path to the application directory
        app_dir = os.path.join(test_dir, '../focus_validator')
        # Set the environment variable for logging level
        env = os.environ.copy()
        env["LOG_LEVEL"] = "INFO"
        
        command = ['poetry', 'run', 'python', os.path.join(app_dir, 'main.py')] + args
        return subprocess.run(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)

    @data(
        # ("fake_focuses500000.csv", 115.0, 500000, "validate_500000_records"),
        # ("fake_focuses250000.csv", 65.0, 250000, "validate_250000_records"),
        # ("fake_focuses100000.csv", 25.0, 100000, "validate_100000_records"),
        # ("fake_focuses50000.csv", 13.0, 50000, "validate_50000_records"),
        # ("fake_focuses10000.csv", 5.0, 10000, "validate_10000_records"),
        # ("fake_focuses5000.csv", 3.5, 5000, "validate_5000_records"),
        ("fake_focuses2000.csv", 2.5, 2000, "validate_2000_records"),
        ("fake_focuses2000.csv", 2.6, 1000, "validate_1000_records")
    )
    @unpack
    def test_param_main_performance(self, file_name, performance_threshold, number_of_records, case_id):
        with self.subTest(case_id=case_id):
            generate_and_write_fake_focuses(file_name, number_of_records)
            self.execute_performance(file_name, performance_threshold)
            if os.path.exists(file_name):
                os.remove(file_name)

    def execute_performance(self, file_name, performance_threshold):
        
        # Get the current directory of this test file
        test_dir = os.path.dirname(os.path.abspath(__file__))
        
        start_time = time.time()

        # Command to execute the focus_validator tool
        result = self.run_validator(['--data-file', os.path.join(test_dir, '../' + file_name)])
        print(result.stdout)

        end_time = time.time()
        duration = end_time - start_time
        logging.info(f"File: {file_name} Duration: {duration} seconds")
        
        self.assertLess(duration, performance_threshold, f"Performance test exceeded threshold. Duration: {duration} seconds")
        self.assertEqual(result.returncode, 0, "Focus Validator did not exit cleanly.")


if __name__ == '__main__':
    unittest.main()