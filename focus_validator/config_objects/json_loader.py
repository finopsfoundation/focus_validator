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

        # ---- build dependency graph (closure + diagnostics) --------------------
        resolver = RuleDependencyResolver(
            dataset_rules=dataset_rules,
            raw_rules_data=rules_dict,
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

