import io
from unittest import TestCase
from uuid import uuid4

from focus_validator.config_objects import Rule, InvalidRule
from focus_validator.config_objects.common import DataTypeCheck, DataTypes
from focus_validator.outputter.outputter_unittest import UnittestOutputter
from focus_validator.rules.spec_rules import ValidationResult


class TestOutputterUnittest(TestCase):
    def test_unittest_output_all_valid_rules(self):
        random_check_id = str(uuid4())
        random_dimension = str(uuid4())

        rules = [
            Rule(
                check_id=random_check_id,
                dimension=random_dimension,
                check=DataTypeCheck(data_type=DataTypes.DECIMAL),
            ),
            Rule(
                check_id=random_check_id,
                dimension=random_dimension,
                check="dimension_required",
            ),
        ]

        _, checklist = Rule.generate_schema(rules=rules)
        result = ValidationResult(checklist=checklist)
        result.process_result()

        buffer = io.BytesIO()
        outputter = UnittestOutputter(output_destination=buffer)
        outputter.write(result_set=result)

        buffer.seek(0)
        output = buffer.read()
        print(output)

        raise ValueError

    def test_unittest_output_with_bad_rule(self):
        rules = [InvalidRule(error="", error_type="", rule_path="")]
        _, checklist = Rule.generate_schema(rules=rules)
