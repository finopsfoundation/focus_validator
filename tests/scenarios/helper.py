import json
import os
from typing import Dict, Any, List, Optional
from focus_validator.config_objects.json_loader import JsonLoader
from focus_validator.config_objects.focus_to_duckdb_converter import FocusToDuckDBSchemaConverter
from focus_validator.config_objects.plan_builder import ValidationPlan, ExecNode
from focus_validator.rules.spec_rules import ValidationResults


def load_rule_data_from_file(filename: str = "base_rule_data.json") -> dict:
    """
    Load rule data from a JSON file with path-safe handling.
    
    Args:
        filename: Name of the JSON file containing rule data
        
    Returns:
        Dictionary containing the rule data
        
    Raises:
        FileNotFoundError: If the file is not found in any expected location
        ValueError: If the JSON is invalid
        RuntimeError: For other loading errors
    """
    # Try multiple possible locations for the rule data file
    possible_paths = [
        os.path.join(os.path.dirname(__file__), filename),  # Same directory as test
        os.path.join(os.path.dirname(__file__), "..", filename),  # Parent directory
        os.path.join(os.getcwd(), filename),  # Current working directory
        os.environ.get("FOCUS_BASE_RULE_FILE", "")  # Environment variable override
    ]
    
    rule_file_path = None
    for path in possible_paths:
        if path and os.path.exists(path):
            rule_file_path = path
            break
    
    if not rule_file_path:
        raise FileNotFoundError(f"Rule data file '{filename}' not found in any of these locations: {possible_paths}")
    
    try:
        with open(rule_file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in rule data file {rule_file_path}: {e}")
    except Exception as e:
        raise RuntimeError(f"Error loading rule data file {rule_file_path}: {e}")

class SpecRulesFromData:
    """
    Alternative to SpecRules that accepts rule data directly instead of loading from files.
    Perfect for CI/CD pipelines where rule data can be embedded or passed programmatically.
    """
    
    def __init__(
        self,
        rule_data: Dict[str, Any],
        focus_dataset: str,
        filter_rules: Optional[str] = None,
        applicability_criteria_list: Optional[List[str]] = None,
    ):
        """
        Initialize with rule data directly.
        
        Args:
            rule_data: Complete FOCUS conformance rules JSON data structure
            focus_dataset: Dataset name (e.g., "CostAndUsage")
            filter_rules: Optional rule filtering
            applicability_criteria_list: Optional applicability criteria
        """
        self.rule_data = rule_data
        self.focus_dataset = focus_dataset
        self.filter_rules = filter_rules
        self.applicability_criteria_list = applicability_criteria_list or []
        self.plan: Optional[ValidationPlan] = None
        self._validated_criteria_cache: Optional[List[str]] = None
        
    def load_rules(self) -> ValidationPlan:
        """Load rules from the provided data (no file I/O)."""
        # Use the same logic as JsonLoader but with in-memory data
        datasets = self.rule_data.get("ModelDatasets", {})
        if self.focus_dataset not in datasets:
            raise ValueError(
                f"Focus dataset '{self.focus_dataset}' not found in rule data"
            )

        dataset = datasets[self.focus_dataset]
        dataset_rules = dataset.get("ModelRules", [])
        rules_dict = self.rule_data.get("ModelRules", {})
        checkfunctions_dict = self.rule_data.get("CheckFunctions", {})
        applicability_criteria_dict = self.rule_data.get("ApplicabilityCriteria", {})

        # Validate applicability criteria (same logic as JsonLoader)
        validated_criteria = []
        if self.applicability_criteria_list:
            # Check if 'ALL' is specified (case insensitive) - must be the only item
            if (
                len(self.applicability_criteria_list) == 1
                and self.applicability_criteria_list[0].upper() == "ALL"
            ):
                validated_criteria = list(applicability_criteria_dict.keys())
            else:
                for criteria in self.applicability_criteria_list:
                    if criteria in applicability_criteria_dict:
                        validated_criteria.append(criteria)

        # Build dependency resolver
        from focus_validator.config_objects.rule_dependency_resolver import RuleDependencyResolver
        
        resolver = RuleDependencyResolver(
            dataset_rules=dataset_rules,
            raw_rules_data=rules_dict,
            validated_applicability_criteria=validated_criteria,
        )

        # Build dependency graph (the constructor already calls collectDatasetRules)
        resolver.buildDependencyGraph(target_rule_prefix=self.filter_rules)
        relevant_rules = resolver.getRelevantRules()

        # Handle rule filtering
        if self.filter_rules:
            entry_rule_ids = [
                rid for rid in relevant_rules.keys() 
                if self.filter_rules in rid
            ]
            if not entry_rule_ids:
                # fallback: allow roots from the raw set if the prefix trimmed relevance too far
                entry_rule_ids = [
                    rid for rid in rules_dict if self.filter_rules in rid
                ]
        else:
            entry_rule_ids = list(relevant_rules.keys())

        # Build validation plan
        val_plan: ValidationPlan = resolver.build_plan_and_schedule(
            entry_rule_ids=entry_rule_ids,
            rules_dict=rules_dict,
            checkfunctions_dict=checkfunctions_dict,
            exec_ctx=None,
        )

        self.plan = val_plan
        self._validated_criteria_cache = validated_criteria  # Cache the validated criteria
        return val_plan
    
    def load(self) -> None:
        """Convenience method for loading rules."""
        self.load_rules()
    
    def _get_validated_criteria(self) -> List[str]:
        """Get the validated applicability criteria, handling ALL expansion."""
        if self._validated_criteria_cache is not None:
            return self._validated_criteria_cache
        
        # If not cached, re-compute (this shouldn't happen normally)
        applicability_criteria_dict = self.rule_data.get("ApplicabilityCriteria", {})
        validated_criteria = []
        if self.applicability_criteria_list:
            if (
                len(self.applicability_criteria_list) == 1
                and self.applicability_criteria_list[0].upper() == "ALL"
            ):
                validated_criteria = list(applicability_criteria_dict.keys())
            else:
                for criteria in self.applicability_criteria_list:
                    if criteria in applicability_criteria_dict:
                        validated_criteria.append(criteria)
        return validated_criteria
    
    def validate(
        self,
        focus_data: Any,
        *,
        connection = None,
        stop_on_first_error: bool = False,
    ) -> ValidationResults:
        """
        Execute validation using the loaded plan.
        Same interface as SpecRules.validate().
        """
        if self.plan is None:
            raise RuntimeError("SpecRulesFromData.validate() called before load_rules().")

        # Force import of real duckdb module (bypass any mocks from other tests)
        import sys
        import importlib
        
        # Temporarily remove mock if present
        mock_backup = sys.modules.get('duckdb', None)
        if mock_backup and hasattr(mock_backup, '_mock_name'):
            del sys.modules['duckdb']
        
        # Import real duckdb
        import duckdb
        
        from typing import Dict
        
        plan = self.plan
        results_by_idx: Dict[int, Dict[str, Any]] = {}
        
        # Get the resolved applicability criteria from the dependency resolver
        # which already handled the "ALL" expansion in load_rules()
        from focus_validator.config_objects.rule_dependency_resolver import RuleDependencyResolver
        resolver = RuleDependencyResolver(
            dataset_rules=self.rule_data.get("ModelDatasets", {}).get(self.focus_dataset, {}).get("ModelRules", []),
            raw_rules_data=self.rule_data.get("ModelRules", {}),
            validated_applicability_criteria=self._get_validated_criteria(),
        )
        
        converter = FocusToDuckDBSchemaConverter(
            focus_data=focus_data,
            validated_applicability_criteria=self._get_validated_criteria(),
            show_violations=False,  # Default for tests
        )
        # Always create a fresh connection to avoid test interference
        # Force connection to None to ensure fresh connection
        fresh_connection = duckdb.connect(":memory:")
        converter.prepare(conn=fresh_connection, plan=plan)

        try:
            # Walk execution layers
            for layer in plan.layers:
                for idx in layer:
                    node: ExecNode = plan.nodes[idx]
                    setattr(
                        node.rule,
                        "_plan_parents_",
                        {
                            plan.nodes[p].rule_id: results_by_idx[p]
                            for p in node.parent_idxs
                        },
                    )
                    
                    parent_results = {
                        pidx: results_by_idx[pidx] for pidx in node.parent_idxs
                    }

                    check = converter.build_check(
                        rule=node.rule,
                        parent_results_by_idx=parent_results,
                        parent_edges=node.parent_edges,
                        rule_id=node.rule_id,
                        node_idx=idx,
                    )

                    ok, details = converter.run_check(check)

                    results_by_idx[idx] = {
                        "ok": ok,
                        "details": details,
                        "rule_id": node.rule_id,
                    }

                    converter._global_results_by_idx[idx] = results_by_idx[idx]

                    if stop_on_first_error and not ok:
                        break
                        
                if stop_on_first_error and idx in results_by_idx and not results_by_idx[idx]["ok"]:
                    break

        finally:
            converter.finalize(success=True, results_by_idx=results_by_idx)
            # Explicitly close the connection we created
            if fresh_connection:
                try:
                    fresh_connection.close()
                except Exception:
                    # Ignore errors during cleanup
                    pass

        rules_dict = {
            self.plan.nodes[i].rule_id: self.plan.nodes[i].rule
            for i in results_by_idx.keys()
        }
        return ValidationResults(
            results_by_idx, self._results_by_rule_id(results_by_idx), rules_dict
        )
    
    def _results_by_rule_id(
        self, by_idx: Dict[int, Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        if self.plan is None:
            return {}
        return {self.plan.nodes[i].rule_id: res for i, res in by_idx.items()}
