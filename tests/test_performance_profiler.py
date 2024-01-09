import cProfile
import csv
import io
import logging
import os
import pstats
import subprocess
import time
import unittest

from tests.samples.csv_random_data_generate_at_scale import generate_and_write_fake_focuses
from focus_validator.validator import Validator

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')


class TestPerformanceProfiler(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Set the environment variable for logging level
        env = os.environ.copy()
        env["LOG_LEVEL"] = "INFO"

        #Generate 1000 fake focuses to a CSV file
        cls.csv_filename_1000 = 'fake_focuses1000.csv'
        cls.csv_filename_10000 = 'fake_focuses10000.csv'
        cls.csv_filename_50000 = 'fake_focuses50000.csv'
        cls.csv_filename_100000 = 'fake_focuses100000.csv'
        cls.csv_filename_250000 = 'fake_focuses250000.csv'
        cls.csv_filename_500000 = 'fake_focuses500000.csv'

        logging.info("Generating file with 1,000 records. Expected time to generate 1.5 seconds")
        cls.generate_test_file(str(cls.csv_filename_1000), 1000)

        # logging.info("Generating file with 10,0000 records. Expected time to generate ~10 seconds")
        # cls.generate_test_file(str(cls.csv_filename_10000), 10000)

        # logging.info("Generating file with 50,0000 records. Expected time to generate ~60 seconds")
        # cls.generate_test_file(str(cls.csv_filename_50000), 50000)

        # logging.info("Generating file with 100,0000 records. Expected time to generate ~120 seconds")
        # cls.generate_test_file(str(cls.csv_filename_100000), 100000)

        # logging.info("Generating file with 250,0000 records. Expected time to generate ~260 seconds")
        # cls.generate_test_file(str(cls.csv_filename_250000), 250000)

        # logging.info("Generating file with 500,0000 records. Expected time to generate ~585 seconds")
        # cls.generate_test_file(str(cls.csv_filename_500000), 500000)

    @classmethod
    def tearDownClass(cls):
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        
        if os.path.exists(os.path.join(base_dir, 'fake_focuses.csv')):
            os.remove(os.path.join(base_dir, 'fake_focuses.csv'))

        if os.path.exists(os.path.join(base_dir, str(cls.csv_filename_1000))):
            os.remove(os.path.join(base_dir, str(cls.csv_filename_1000)))
        
        if os.path.exists(os.path.join(base_dir, str(cls.csv_filename_10000))):
            os.remove(os.path.join(base_dir, str(cls.csv_filename_10000)))
        
        if os.path.exists(os.path.join(base_dir, str(cls.csv_filename_50000))):
            os.remove(os.path.join(base_dir, str(cls.csv_filename_50000)))
        
        if os.path.exists(os.path.join(base_dir, str(cls.csv_filename_100000))):
            os.remove(os.path.join(base_dir, str(cls.csv_filename_100000)))
        
        if os.path.exists(os.path.join(base_dir, str(cls.csv_filename_250000))):
            os.remove(os.path.join(base_dir, str(cls.csv_filename_250000)))
        
        if os.path.exists(os.path.join(base_dir, str(cls.csv_filename_500000))):
            os.remove(os.path.join(base_dir, str(cls.csv_filename_500000)))
    
    @classmethod
    def generate_test_file(cls, csv_filename, number_of_records):
        #Generate fake focuses to a CSV file
        # fake_focuses = generate_fake_focus(number_of_records)

        # write_fake_focuses_to_csv(fake_focuses, csv_filename)
        generate_and_write_fake_focuses(csv_filename, number_of_records)
    
    
    def test_1000_record_csv_performance(self):
        self.execute_profiler(str(self.csv_filename_1000), 5.0)

    # def test_10000_record_csv_performance(self):
    #     self.execute_profiler(str(self.csv_filename_10000), 10.0)
    
    # def test_50000_record_csv_performance(self):
    #     self.execute_profiler(str(self.csv_filename_50000), 25.0)

    # def test_100000_record_csv_performance(self):
    #     self.execute_profiler(str(self.csv_filename_100000), 50.0)
    
    # def test_250000_record_csv_performance(self):
    #     self.execute_profiler(str(self.csv_filename_250000), 100.0)
        
    # def test_500000_record_csv_performance(self):
    #     self.execute_profiler(str(self.csv_filename_500000), 120.0)
    
    def profile_to_csv(self, profiling_result, csv_file):
        with open(csv_file, 'w', newline='') as f:
            w = csv.writer(f)
            # Write the headers
            headers = ['ncalls', 'tottime', 'percall', 'cumtime', 'percall', 'filename:lineno(function)']
            w.writerow(headers)
        
            # Write each row
            for row in profiling_result.stats.items():
                func_name, (cc, nc, tt, ct, callers) = row
                w.writerow([nc, tt, tt/nc, ct, ct/cc, func_name])

    def execute_profiler(self, file_name, performance_threshold):
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

    
    
if __name__ == '__main__':
    unittest.main()