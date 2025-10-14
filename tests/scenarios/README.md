# FOCUS Validator Scenario Tests

This directory contains scenario-based unit tests for the FOCUS validator using in-memory CSV data stored in Python objects.

## Test Structure

Both test files follow the same pattern:

1. **CSV Data in Python**: Use `StringIO(csv_data)` to create in-memory CSV data
2. **Pandas Integration**: Convert to DataFrame with `pd.read_csv(StringIO(csv_data))`
3. **Targeted Validation**: Use `filter_rules` parameter to test specific rule sets
4. **Rule State Validation**: Check for PASS/FAIL states using `results.by_rule_id[rule_id].get("ok")`

## Usage Pattern

```python
import unittest
import pandas as pd
from io import StringIO
from focus_validator.rules.spec_rules import SpecRules

class TestExample(unittest.TestCase):
    def setUp(self):
        self.spec_rules = SpecRules(
            spec_version="1.2",
            filter_rules=["your-rule-id"]
        )

    def test_rule_scenario(self):
        csv_data = """Column1,Column2,Column3
"value1","value2","value3"
"""
        df = pd.read_csv(StringIO(csv_data))
        results = self.spec_rules.validate(focus_data=df)
        
        # Check rule state
        rule_result = results.by_rule_id["your-rule-id"]
        self.assertTrue(rule_result.get("ok"), f"Rule should PASS but got: {rule_result}")
```

## Key Points

1. **Rule IDs**: Always verify actual rule IDs in the codebase (e.g., `BilledCost-C-005-C` not `C-004-C`)
2. **Column Requirements**: Column presence rules require all mandatory FOCUS columns
3. **Targeted Testing**: Use `filter_rules` to focus on specific rules and avoid column presence issues
4. **Result Structure**: `ValidationResults.by_rule_id` provides dictionary access to rule outcomes
5. **Empty Data**: BilledCost rules typically pass with empty data (no violations possible)

## Running Tests

```bash
# Run all scenario tests
poetry run python -m pytest tests/scenarios/ -v

# Run specific test file
poetry run python -m pytest tests/scenarios/test_conditional_rules.py -v
```

All tests use the established CSV-in-memory pattern and validate specific rule PASS/FAIL states as requested.
