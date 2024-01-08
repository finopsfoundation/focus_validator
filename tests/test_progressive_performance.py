import logging
import os
import subprocess
import time
import unittest

from tests.samples.csv_random_data_generate_at_scale import generate_and_write_fake_focuses

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')


class TestProgressivePerformance(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        #Generate 1000 fake focuses to a CSV file
        cls.csv_filename_1000 = 'fake_focuses1000.csv'
        cls.csv_filename_10000 = 'fake_focuses10000.csv'
        cls.csv_filename_50000 = 'fake_focuses50000.csv'
        cls.csv_filename_100000 = 'fake_focuses100000.csv'
        cls.csv_filename_250000 = 'fake_focuses250000.csv'
        cls.csv_filename_500000 = 'fake_focuses500000.csv'

        logging.info("Generating file with 1,000 records")
        cls.generate_test_file(str(cls.csv_filename_1000), 1000)

        # logging.info("Generating file with 10,0000 records")
        # cls.generate_test_file(str(cls.csv_filename_10000), 10000)

        # logging.info("Generating file with 50,0000 records")
        # cls.generate_test_file(str(cls.csv_filename_50000), 50000)

        # logging.info("Generating file with 100,0000 records")
        # cls.generate_test_file(str(cls.csv_filename_100000), 100000)

        # logging.info("Generating file with 250,0000 records")
        # cls.generate_test_file(str(cls.csv_filename_250000), 250000)

        # logging.info("Generating file with 500,0000 records")
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

    def test_1000_record_csv_performance(self):
        self.execute_performance(str(self.csv_filename_1000), 25.0)

    # def test_10000_record_csv_performance(self):
    #     self.execute_performance(str(self.csv_filename_10000), 25.0)
    
    # def test_50000_record_csv_performance(self):
    #     self.execute_performance(str(self.csv_filename_50000), 150.0)

    # def test_100000_record_csv_performance(self):
    #     self.execute_performance(str(self.csv_filename_100000), 300.0)
    
    # def test_250000_record_csv_performance(self):
    #     self.execute_performance(str(self.csv_filename_250000), 300.0)
        
    # def test_500000_record_csv_performance(self):
    #     self.execute_performance(str(self.csv_filename_500000), 300.0)
    
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