from typing import Dict, List, Set, Any, Optional
from collections import defaultdict, deque


class RuleDependencyResolver:
    # Build a DAG and do Topological sort to get dependencies

    def __init__(self, rules_data: Dict[str, Any]):
        self.rules_data = rules_data
        self.dependency_graph = defaultdict(list)  # rule_id -> [dependent_rule_ids]
        self.reverse_graph = defaultdict(list)  # rule_id -> [rules_that_depend_on_this]
        self.in_degree = defaultdict(int)  # rule_id -> number of dependencies

    def buildDependencyGraph(self, target_rule_prefix: Optional[str] = "BilledCost") -> None:
        """
        Build dependency graph for rules with the given prefix.
        If target_rule_prefix is None, processes all rules.
        Otherwise, only processes rules that start with target_rule_prefix.
        """
        # Filter rules by prefix first (or use all rules if prefix is None)
        if target_rule_prefix is None:
            filtered_rules = self.rules_data
        else:
            filtered_rules = {
                rule_id: rule_data
                for rule_id, rule_data in self.rules_data.items()
                if rule_id.startswith(target_rule_prefix)
            }

        # Build graph for filtered rules
        for rule_id, rule_data in filtered_rules.items():
            dependencies = self._extractRuleDependencies(rule_data)

            # Only add dependencies that are also in our filtered set
            filtered_dependencies = [
                dep for dep in dependencies
                if dep in filtered_rules
            ]

            self.dependency_graph[rule_id] = filtered_dependencies
            self.in_degree[rule_id] = len(filtered_dependencies)

            # Build reverse graph
            for dependency in filtered_dependencies:
                self.reverse_graph[dependency].append(rule_id)

    def _extractRuleDependencies(self, rule_data: Dict[str, Any]) -> List[str]:
        """
        Extract ConformanceRule dependencies from ValidationCriteria.
        Prioritizes explicit Dependencies field, then falls back to parsing Requirements/Conditions.
        """
        dependencies = []
        validation_criteria = rule_data.get("ValidationCriteria", {})

        # Check explicit Dependencies field first - this is the preferred method
        explicit_deps = validation_criteria.get("Dependencies", [])
        if explicit_deps:
            dependencies.extend(explicit_deps)
        else:
            # Fall back to parsing Requirements and Conditions for legacy compatibility
            requirement = validation_criteria.get("Requirement", {})
            if requirement:
                dependencies.extend(self._extractDependenciesFromRequirement(requirement))

            # Also check for dependencies in Condition if present
            condition = validation_criteria.get("Condition", {})
            if condition:
                dependencies.extend(self._extractDependenciesFromRequirement(condition))

        return list(set(dependencies))  # Remove duplicates

    def _extractDependenciesFromRequirement(self, requirement: Dict[str, Any]) -> List[str]:
        """
        Recursively extract dependencies from a requirement structure.
        Handles CheckConformanceRule, AND, and OR operations.
        """
        dependencies = []

        check_function = requirement.get("CheckFunction")

        if check_function == "CheckConformanceRule":
            conformance_rule_id = requirement.get("ConformanceRuleId")
            if conformance_rule_id:
                dependencies.append(conformance_rule_id)

        elif check_function in ["AND", "OR"]:
            items = requirement.get("Items", [])
            for item in items:
                if isinstance(item, dict):
                    dependencies.extend(self._extractDependenciesFromRequirement(item))

        return dependencies

    def getTopologicalOrder(self) -> List[str]:
        """
        Perform topological sort using Kahn's algorithm.
        Returns rules in order such that dependencies are processed before dependents.
        """
        # Copy in_degree to avoid modifying original
        in_degree_copy = self.in_degree.copy()
        queue = deque()
        result = []

        # Find all rules with no dependencies
        for rule_id in in_degree_copy:
            if in_degree_copy[rule_id] == 0:
                queue.append(rule_id)

        while queue:
            current_rule = queue.popleft()
            result.append(current_rule)

            # Process all rules that depend on current_rule
            for dependent in self.reverse_graph[current_rule]:
                in_degree_copy[dependent] -= 1
                if in_degree_copy[dependent] == 0:
                    queue.append(dependent)

        # Check for circular dependencies
        if len(result) != len(self.in_degree):
            remaining_rules = [rule for rule in self.in_degree if rule not in result]
            raise ValueError(f"Circular dependency detected among rules: {remaining_rules}")

        return result

    def getDependencies(self, rule_id: str) -> List[str]:
        """Get direct dependencies for a specific rule."""
        return self.dependency_graph.get(rule_id, [])

    def getDependents(self, rule_id: str) -> List[str]:
        """Get rules that depend on the specified rule."""
        return self.reverse_graph.get(rule_id, [])

    def isCompositeRule(self, rule_id: str) -> bool:
        """
        Check if a rule is composite by examining its Function type
        and whether it has CheckConformanceRule dependencies.
        """
        rule_data = self.rules_data.get(rule_id, {})
        function_type = rule_data.get("Function")
        has_dependencies = len(self.getDependencies(rule_id)) > 0

        return function_type == "Composite" and has_dependencies

    def getCompositeRuleLogic(self, rule_id: str) -> Optional[str]:
        """
        Extract the logic (AND/OR) for a composite rule.
        """
        if not self.isCompositeRule(rule_id):
            return None

        rule_data = self.rules_data.get(rule_id, {})
        validation_criteria = rule_data.get("ValidationCriteria", {})
        requirement = validation_criteria.get("Requirement", {})

        return requirement.get("CheckFunction")  # Should be "AND" or "OR"
