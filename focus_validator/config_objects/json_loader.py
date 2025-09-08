import json
import os
from collections import OrderedDict
from typing import Dict, Any, List, Optional
from focus_validator.config_objects.rule_dependency_resolver import RuleDependencyResolver


class JsonLoader:

    @staticmethod
    def load_json_rules(json_file_path: str) -> tuple[Dict[str, Any], OrderedDict[str, Any]]:
        if not os.path.exists(json_file_path):
            raise FileNotFoundError(f"JSON rules file not found: {json_file_path}")

        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        rules_dict = data.get('ConformanceRules', {})

        checkfunctions_data = data.get('CheckFunctions', {})
        checkfunctions_ordered_dict = OrderedDict(checkfunctions_data)

        return rules_dict, checkfunctions_ordered_dict

    @staticmethod
    def load_json_rules_with_dependencies(json_file_path: str, rule_prefix: Optional[str] = "BilledCost") -> tuple[Dict[str, Any], OrderedDict[str, Any], List[str]]:
        rules_dict, checkfunctions_dict = JsonLoader.load_json_rules(json_file_path)

        # Create dependency resolver and build graph
        resolver = RuleDependencyResolver(rules_dict)
        resolver.buildDependencyGraph(rule_prefix)

        # Get topological order for rule processing
        rule_order = resolver.getTopologicalOrder()

        return rules_dict, checkfunctions_dict, rule_order

    @staticmethod
    def getRuleDependencies(rules_dict: Dict[str, Any], rule_prefix: str = "BilledCost") -> RuleDependencyResolver:
        resolver = RuleDependencyResolver(rules_dict)
        resolver.buildDependencyGraph(rule_prefix)
        return resolver
