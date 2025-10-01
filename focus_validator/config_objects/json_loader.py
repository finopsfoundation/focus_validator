import json
import os
import logging
from collections import OrderedDict
from typing import Dict, Any, List, Optional
from focus_validator.config_objects.rule_dependency_resolver import RuleDependencyResolver
from .plan_builder import ValidationPlan

class JsonLoader:
    log = logging.getLogger(f"{__name__}.{__qualname__}")
    @staticmethod
    def load_json_rules(json_rule_file: str) -> tuple[Dict[str, Any], OrderedDict[str, Any]]:
        if not os.path.exists(json_rule_file):
            raise FileNotFoundError(f"JSON rules file not found: {json_rule_file}")

        with open(json_rule_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data

    @staticmethod
    def load_json_rules_with_dependencies(
        json_rule_file: str,
        focus_dataset: Optional[str] = "",
        filter_rules: Optional[str] = None,
        applicability_criteria_list: Optional[List[str]] = None,
    ) -> ValidationPlan:
        """
        Load CR JSON, build the dependency graph with RuleDependencyResolver,
        select relevant rules, and return an execution-ready ValidationPlan
        (parents preserved, topo-ordered nodes + layers).
        """
        cr_data = JsonLoader.load_json_rules(json_rule_file)

        # ---- dataset + base maps ------------------------------------------------
        datasets = cr_data.get("ConformanceDatasets", {})
        if focus_dataset not in datasets:
            raise ValueError(
                f"Focus dataset '{focus_dataset}' not found in rules file '{json_rule_file}'"
            )

        dataset = datasets[focus_dataset]
        dataset_rules = dataset.get("ConformanceRules", [])
        rules_dict = cr_data.get("ConformanceRules", {})
        checkfunctions_dict = OrderedDict(cr_data.get("CheckFunctions", {}))
        applicability_criteria_dict = OrderedDict(cr_data.get("ApplicabilityCriteria", {}))

        # ---- validate and filter applicability criteria ----------------------
        validated_criteria = []
        if applicability_criteria_list:
            # Check if 'ALL' is specified (case insensitive)
            if len(applicability_criteria_list) == 1 and applicability_criteria_list[0].upper() == 'ALL':
                validated_criteria = list(applicability_criteria_dict.keys())
                JsonLoader.log.info("Using ALL applicability criteria (%d total): %s", len(validated_criteria), validated_criteria)
            else:
                for criteria in applicability_criteria_list:
                    if criteria in applicability_criteria_dict:
                        validated_criteria.append(criteria)
                        JsonLoader.log.info("Using applicability criteria: %s", criteria)
                    else:
                        JsonLoader.log.warning("Applicability criteria '%s' not found in rules file. Available: %s", 
                                             criteria, list(applicability_criteria_dict.keys()))
                if not validated_criteria:
                    JsonLoader.log.warning("No valid applicability criteria found. Rules with applicability criteria will be skipped.")
        else:
            # If no criteria specified, pass empty list (rules with criteria will be skipped)
            JsonLoader.log.info("No applicability criteria specified. Rules with applicability criteria will be skipped.")
            validated_criteria = []

        # ---- build dependency graph (closure + diagnostics) --------------------
        resolver = RuleDependencyResolver(
            dataset_rules=dataset_rules,
            raw_rules_data=rules_dict,
            validated_applicability_criteria=validated_criteria,
        )
        resolver.buildDependencyGraph(target_rule_prefix=filter_rules)
        relevant_rules = resolver.getRelevantRules()  # Dict[str, ConformanceRule]

        # ---- choose roots for the plan ------------------------------------------
        # If a filter prefix is provided, prefer roots drawn from the relevant set first.
        if filter_rules:
            entry_rule_ids: List[str] = [rid for rid in relevant_rules if rid.startswith(filter_rules)]
            if not entry_rule_ids:
                # fallback: allow roots from the raw set if the prefix trimmed relevance too far
                entry_rule_ids = [rid for rid in rules_dict if rid.startswith(filter_rules)]
        else:
            entry_rule_ids = list(relevant_rules.keys())

        # ---- build plan + compile to ValidationPlan ------------------------------
        val_plan: ValidationPlan = resolver.build_plan_and_schedule(
            entry_rule_ids=entry_rule_ids,
            rules_dict=rules_dict,
            checkfunctions_dict=checkfunctions_dict,
            exec_ctx=None,  # supply a runtime context later if you want gated edges
        )

        return val_plan

