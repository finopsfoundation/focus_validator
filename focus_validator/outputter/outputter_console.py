import logging
import sys
from typing import Any, Dict

# from focus_validator.rules.spec_rules import ValidationResults  # wherever it lives

STATUS_PASS = "PASS"
STATUS_FAIL = "FAIL"
STATUS_SKIP = "SKIPPED"


def _get_safe_icons():
    """Get console-safe icons based on the system's encoding capabilities."""
    try:
        # Test if we can encode Unicode characters
        test_chars = "✅⏭️❌"
        if hasattr(sys.stdout, "encoding") and sys.stdout.encoding:
            test_chars.encode(sys.stdout.encoding)
        return {"pass": "✅", "skip": "⏭️", "fail": "❌"}
    except (UnicodeEncodeError, LookupError):
        # Fall back to ASCII-safe characters
        return {"pass": "[PASS]", "skip": "[SKIP]", "fail": "[FAIL]"}


def _status_from_result(entry: Dict[str, Any]) -> str:
    # entry = {"ok": bool, "details": {...}, "rule_id": "..."}
    details = entry.get("details") or {}
    if details.get("skipped"):
        return STATUS_SKIP
    return STATUS_PASS if entry.get("ok") else STATUS_FAIL


def _line_for_rule(rule_id: str, entry: Dict[str, Any]) -> str:
    status = _status_from_result(entry)
    details = entry.get("details") or {}
    extra = []

    # add useful extras if present
    if "violations" in details:
        extra.append(f"violations={details['violations']}")
    if details.get("reason"):
        extra.append(f"reason={details['reason']}")
    if details.get("message"):
        extra.append(f"msg={details['message']}")
    if "timing_ms" in details:
        extra.append(f"{details['timing_ms']:.1f}ms")

    tail = f"  ({', '.join(extra)})" if extra else ""
    icons = _get_safe_icons()
    icon = (
        icons["pass"]
        if status == STATUS_PASS
        else (icons["skip"] if status == STATUS_SKIP else icons["fail"])
    )
    return f"{icon} {rule_id}: {status}{tail}"


class ConsoleOutputter:
    def __init__(self, output_destination, show_violations=False):
        self.log = logging.getLogger(f"{__name__}.{self.__class__.__qualname__}")
        self.output_destination = output_destination
        self.result_set = None
        self.show_violations = show_violations

    def write(self, results) -> None:
        """
        results: ValidationResults (new type)
        """
        # Expect the new API; fail loudly if the old one is passed
        if not hasattr(results, "by_rule_id"):
            raise TypeError(
                "ConsoleOutputter.write expected ValidationResults with by_rule_id"
            )

        # Summary counts
        passed = failed = skipped = 0
        lines = []

        for rule_id, entry in results.by_rule_id.items():
            status = _status_from_result(entry)
            if status == STATUS_PASS:
                passed += 1
            elif status == STATUS_FAIL:
                failed += 1
            else:
                skipped += 1
            lines.append(_line_for_rule(rule_id, entry))

        # stable ordering helps in consoles
        lines.sort()

        # Print
        print("\n=== Validation Results ===")
        print(
            f"Total: {passed + failed + skipped} | "
            f"Pass: {passed} | Fail: {failed} | Skipped: {skipped}"
        )
        for line in lines:
            print(line)

        # Optional: print a small failures section with reasons
        if failed:
            print("\n--- Failures ---")
            for rule_id, entry in results.by_rule_id.items():
                if _status_from_result(entry) != STATUS_FAIL:
                    continue
                d = entry.get("details") or {}
                msg = d.get("message") or d.get("reason") or f"{rule_id} failed"
                vio = d.get("violations", "?")

                # Access MustSatisfy from the rule object
                rule = results.rules.get(rule_id)
                must_satisfy = rule.validation_criteria.must_satisfy if rule else "N/A"

                print(f"- {rule_id}: violations={vio}; {msg}")
                print(f"  MustSatisfy: {must_satisfy}")

                # Show sample violation data if --show-violations is enabled and data exists
                if self.show_violations and "failure_cases" in d:
                    failure_cases = d["failure_cases"]
                    if hasattr(failure_cases, "shape") and failure_cases.shape[0] > 0:
                        print("  Sample violations:")
                        # Show up to 2 sample rows
                        for i in range(min(2, failure_cases.shape[0])):
                            row_data = failure_cases.iloc[i]
                            # Display each column value in the violating row
                            violation_values = []
                            for col_name, value in row_data.items():
                                violation_values.append(f"{col_name}='{value}'")
                            print(f"    {', '.join(violation_values)}")
                elif self.show_violations and "sample_error" in d:
                    print(f"  Sample violation error: {d['sample_error']}")
