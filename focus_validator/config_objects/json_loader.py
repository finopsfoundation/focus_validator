import json
import os
from collections import OrderedDict
from typing import Dict, Any, List, Optional
from focus_validator.config_objects.rule_dependency_resolver import RuleDependencyResolver


class JsonLoader:

    @staticmethod
    def load_json_rules(json_rule_file: str) -> tuple[Dict[str, Any], OrderedDict[str, Any]]:
        if not os.path.exists(json_rule_file):
            raise FileNotFoundError(f"JSON rules file not found: {json_rule_file}")

        with open(json_rule_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        rules_dict = data.get('ConformanceRules', {})

        checkfunctions_data = data.get('CheckFunctions', {})
        checkfunctions_ordered_dict = OrderedDict(checkfunctions_data)

        return rules_dict, checkfunctions_ordered_dict

    @staticmethod
    def load_json_rules_with_dependencies(json_rule_file: str, rule_prefix: Optional[str] = "") -> tuple[Dict[str, Any], OrderedDict[str, Any], List[str]]:
        rules_dict, checkfunctions_dict = JsonLoader.load_json_rules(json_rule_file)

        # Create dependency resolver and build graph
        resolver = RuleDependencyResolver(rules_dict)
        resolver.buildDependencyGraph(rule_prefix)

        # Get topological order for rule processing
        rule_order = resolver.getTopologicalOrder()

        return rules_dict, checkfunctions_dict, rule_order

    @staticmethod
    def getRuleDependencies(rules_dict: Dict[str, Any], rules_prefix: str = "") -> RuleDependencyResolver:
        resolver = RuleDependencyResolver(rules_dict)
        resolver.buildDependencyGraph(rules_prefix)
        return resolver
