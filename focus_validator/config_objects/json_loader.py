import json
import os
import logging
from collections import OrderedDict
from typing import Dict, Any, List, Optional
from focus_validator.config_objects.rule_dependency_resolver import RuleDependencyResolver


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
    def load_json_rules_with_dependencies(json_rule_file: str, focus_dataset: Optional[str] = "", filter_rules: Optional[str] = None) -> tuple[Dict[str, Any], OrderedDict[str, Any], List[str]]:
        cr_data = JsonLoader.load_json_rules(json_rule_file)

        if focus_dataset not in cr_data.get('ConformanceDatasets', {}):
            raise ValueError(f"Focus dataset '{focus_dataset}' not found in rules file '{json_rule_file}'")
        dataset = cr_data['ConformanceDatasets'][focus_dataset]
        dataset_rules = dataset.get('ConformanceRules', [])
        rules_dict = cr_data.get('ConformanceRules', {})
        checkfunctions_data = cr_data.get('CheckFunctions', {})
        checkfunctions_dict = OrderedDict(checkfunctions_data)

        # Create dependency resolver and build graph
        resolver = RuleDependencyResolver(dataset_rules=dataset_rules, raw_rules_data=rules_dict)
        resolver.buildDependencyGraph(target_rule_prefix=filter_rules)

        # Get topological order for rule processing
        rule_order = resolver.getTopologicalOrder()

        return rules_dict, checkfunctions_dict, rule_order

