import os
import subprocess
import time
import unittest

class TestFocusValidatorFunctionality(unittest.TestCase):
    def run_validator(self, args):
        # Get the current directory of this test file
        test_dir = os.path.dirname(os.path.abspath(__file__))

        # Construct the path to the application directory
        app_dir = os.path.join(test_dir, '../focus_validator')

        command = ['poetry', 'run', 'python', os.path.join(app_dir, 'main.py')] + args
        return subprocess.run(command, capture_output=True, text=True, check=True)

    def test_with_valid_data_file(self):
        # Get the current directory of this test file
        test_dir = os.path.dirname(os.path.abspath(__file__))

        # Test with a valid data file
        result = self.run_validator(['--data-file', os.path.join(test_dir, 'samples/all_pass.csv')])
        self.assertEqual(result.returncode, 0, "The script should exit cleanly with a valid data file.")

    def test_all_pass_csv_performance(self):
        

        # Get the current directory of this test file
        test_dir = os.path.dirname(os.path.abspath(__file__))
        
        start_time = time.time()

        # Command to execute the focus_validator tool
        result = self.run_validator(['--data-file', os.path.join(test_dir, 'fake_focuses.csv')])

        end_time = time.time()
        duration = end_time - start_time
        print(duration)
        # Set a performance threshold in seconds
        performance_threshold = 25.0  # Example threshold

        self.assertLess(duration, performance_threshold, f"Performance test exceeded threshold. Duration: {duration} seconds")
        self.assertEqual(result.returncode, 0, "Focus Validator did not exit cleanly.")


if __name__ == '__main__':
    unittest.main()