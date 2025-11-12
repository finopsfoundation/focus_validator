from unittest import TestCase
from unittest.mock import patch, MagicMock

from focus_validator.main import main
from focus_validator.validator import Validator
from focus_validator.rules.spec_rules import ValidationResults


class TestMainFunction(TestCase):
    def test_supported_versions(self):
        with patch("sys.argv", ["prog", "--supported-versions", "--block-download"]):
            try:
                main()
            except SystemExit as e:
                self.assertNotEqual(e.code, 2)

    @patch.object(Validator, "validate")
    def test_data_file(self, mock_validate):
        mock_validate.return_value = ({}, MagicMock(), ValidationResults(
            {}, {}, {}, "test_rules", "test_data.csv", 0, "test_model", "CostAndUsage"
        ))
        with patch("sys.argv", ["prog", "--data-file", "path/to/data.csv", "--block-download"]):
            try:
                main()
            except SystemExit as e:
                self.assertNotEqual(e.code, 2)
