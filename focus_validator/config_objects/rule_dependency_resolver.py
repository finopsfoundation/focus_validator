import logging
from typing import Dict, List, Set, Any, Optional, Iterable, Tuple
from collections import defaultdict, deque
from .rule import ConformanceRule
from .plan_builder import PlanBuilder, compile_validation_plan, default_key_fn, ValidationPlan


log = logging.getLogger(__name__)

def _tarjan_scc(graph: Dict[str, Set[str]]) -> List[List[str]]:
    r"""Tarjan SCC for cycle diagnostics."""
    index = 0
    stack: List[str] = []
    onstack: Set[str] = set()
    indices: Dict[str, int] = {}
    lowlink: Dict[str, int] = {}
    sccs: List[List[str]] = []

    def strongconnect(v: str):
        nonlocal index
        indices[v] = index
        lowlink[v] = index
        index += 1
        stack.append(v); onstack.add(v)
        for w in graph.get(v, ()):
            if w not in indices:
                strongconnect(w)
                lowlink[v] = min(lowlink[v], lowlink[w])
            elif w in onstack:
                lowlink[v] = min(lowlink[v], indices[w])
        if lowlink[v] == indices[v]:
            comp: List[str] = []
            while True:
                w = stack.pop(); onstack.discard(w)
                comp.append(w)
                if w == v: break
            sccs.append(comp)

    for v in graph.keys():
        if v not in indices:
            strongconnect(v)
    return sccs

def _build_reverse_graph(graph: Dict[str, Set[str]]) -> Dict[str, Set[str]]:
    rev: Dict[str, Set[str]] = defaultdict(set)
    for a, to_set in graph.items():
        for b in to_set:
            rev[b].add(a)
    return rev

def _log_graph_snapshot(graph: Dict[str, Set[str]], *, name: str = "rule-graph", sample: int = 10) -> None:
    node_count = len(graph)
    edge_count = sum(len(v) for v in graph.values())
    zeros = [n for n, deps in graph.items() if not deps]
    log.debug("%s: nodes=%d, edges=%d, zero-incoming (prereqs=0) count=%d",
             name, node_count, edge_count, len(zeros))
    if zeros:
        log.debug("sample zero-prereq nodes: %s", ", ".join(sorted(zeros)[:sample]))
    # Show a few edges
    shown = 0
    for n, deps in graph.items():
        if deps:
            log.debug("edge(s): %s <- %s", n, ", ".join(sorted(deps)))
            shown += 1
            if shown >= sample:
                break

def _export_dot(graph: Dict[str, Set[str]], path: str) -> None:
    r"""Write a Graphviz DOT file for visualization (dependency -> dependent)."""
    rev = _build_reverse_graph(graph)
    with open(path, "w", encoding="utf-8") as f:
        f.write("digraph G {\n")
        f.write('  rankdir=LR;\n  node [shape=box, fontsize=10];\n')
        for dep, dependents in rev.items():
            for d in sorted(dependents):
                # dep -> d (dep is prerequisite of d)
                f.write(f'  "{dep}" -> "{d}";\n')
        f.write("}\n")
    log.debug("Wrote DOT graph to %s", path)

def _log_sccs(graph: Dict[str, Set[str]], *, top_k: int = 10) -> List[List[str]]:
    sccs = _tarjan_scc(graph)
    cycles = [c for c in sccs if len(c) > 1]
    if cycles:
        log.warning("Detected %d cycle component(s). Showing up to %d:", len(cycles), top_k)
        for i, comp in enumerate(sorted(cycles, key=len, reverse=True)[:top_k], 1):
            log.warning("  Cycle %d (size %d): %s", i, len(comp), ", ".join(sorted(comp)))
    else:
        log.debug("No cycles found by Tarjan SCC.")
    return cycles

def _restrict_graph(graph: Dict[str, Set[str]], nodes: Set[str]) -> Dict[str, Set[str]]:
    return {n: set(d for d in graph.get(n, ()) if d in nodes) for n in nodes}

def _find_simple_cycle(graph: Dict[str, Set[str]], nodes: List[str]) -> Optional[List[str]]:
    r"""Return one simple cycle within the subgraph induced by `nodes`, if any."""
    target = set(nodes)
    visited: Set[str] = set()
    stack: List[str] = []
    onpath: Set[str] = set()

    def dfs(u: str) -> Optional[List[str]]:
        visited.add(u)
        stack.append(u)
        onpath.add(u)
        for v in graph.get(u, ()):
            if v not in target:
                continue
            if v not in visited:
                cyc = dfs(v)
                if cyc: return cyc
            elif v in onpath:
                # reconstruct cycle v -> ... -> u -> v
                idx = len(stack) - 1
                while idx >= 0 and stack[idx] != v:
                    idx -= 1
                if idx >= 0:
                    return stack[idx:] + [v]
        onpath.remove(u)
        stack.pop()
        return None

    for n in nodes:
        if n not in visited:
            cyc = dfs(n)
            if cyc:
                return cyc
    return None

def _export_scc_dot(graph: Dict[str, Set[str]], comp: List[str], idx: int, path_prefix: str = "rule_graph_scc") -> str:
    sg = _restrict_graph(graph, set(comp))
    dot_path = f"{path_prefix}_{idx}.dot"
    with open(dot_path, "w", encoding="utf-8") as f:
        f.write("digraph G {\nrankdir=LR;\nnode [shape=box, fontsize=10];\n")
        for a, to_set in sorted(sg.items()):
            if not to_set:
                f.write(f'"{a}";\n')
            for b in sorted(to_set):
                f.write(f'"{a}" -> "{b}";\n')
        f.write("}\n")
    log.debug("Wrote SCC #%d DOT to %s", idx, dot_path)
    return dot_path

def _log_cycle_details(graph: Dict[str, Set[str]], cycles: List[List[str]], *, top_k: int = 10) -> None:
    r"""For each SCC cycle, dump adjacency within the SCC, a simple cycle path, and write a DOT subgraph."""
    for i, comp in enumerate(sorted(cycles, key=len, reverse=True)[:top_k], 1):
        log.warning("=== Cycle %d detail (size %d) ===", i, len(comp))
        comp_set = set(comp)
        # Adjacency within SCC
        for n in sorted(comp):
            outs = sorted(d for d in graph.get(n, ()) if d in comp_set)
            log.warning("  %s depends on: %s", n, ", ".join(outs) if outs else "(none?)")
        # Example simple cycle path
        cyc = _find_simple_cycle(graph, comp)
        if cyc:
            log.warning("  Example cycle path: %s", " -> ".join(cyc))
        else:
            log.warning("  Could not reconstruct a simple cycle path (unexpected).")
        # DOT export
        try:
            _export_scc_dot(graph, comp, i)
        except Exception as e:
            log.exception("  Failed to export DOT for SCC %d: %s", i, e)

def _trace_node(graph: Dict[str, Set[str]], node: str, *, max_depth: int = 4) -> None:
    r"""Log a bounded DFS of prerequisites for a node to see why its in-degree never reaches 0."""
    seen: Set[str] = set()
    stack: List[Tuple[str, int]] = [(node, 0)]
    log.debug("Tracing prerequisites for %s (depth<=%d)", node, max_depth)
    while stack:
        cur, depth = stack.pop()
        if cur in seen: 
            continue
        seen.add(cur)
        if depth == max_depth:
            log.debug("%s%s (…)", "  " * depth, cur)
            continue
        log.debug("%s%s", "  " * depth, cur)
        for dep in sorted(graph.get(cur, ())):
            stack.append((dep, depth + 1))

def _dump_blockers(graph: Dict[str, Set[str]], remaining: Iterable[str]) -> None:
    r"""For each remaining node after Kahn, log its prereqs (with counts) to reveal blockers."""
    for n in sorted(remaining):
        deps = sorted(graph.get(n, ()))
        log.warning("BLOCKED: %s needs %d prereq(s): %s", n, len(deps), ", ".join(deps))

# === End instrumentation utilities ===========================================



class RuleDependencyResolver:
    # Build a DAG and do Topological sort to get dependencies

    def __init__(self, dataset_rules: Dict[str, Any], raw_rules_data: Dict[str, Any], validated_applicability_criteria: Optional[List[str]] = None):
        self.log = logging.getLogger(f"{__name__}.{self.__class__.__qualname__}")
        self.dataset_rules = dataset_rules
        self.validated_applicability_criteria = validated_applicability_criteria or []
        self.rules = self.collectDatasetRules(raw_rules_data)
        self.dependency_graph = defaultdict(set)  # rule_id -> {dependent_rule_ids}
        self.reverse_graph = defaultdict(list)  # rule_id -> [rules_that_depend_on_this]
        self.in_degree = defaultdict(int)  # rule_id -> number of dependencies



    def collectDatasetRules(self, raw_rules_data: Dict[str, Any]) -> Dict[str, Any]:
        """Collect rules relevant to the specified dataset."""
        if not self.dataset_rules:
            raise ValueError("No dataset rules provided to collectDatasetRules.")

        relevant_rules = {}
        dependencies = deque()

        for rule_id in self.dataset_rules:
            rule_data = raw_rules_data.get(rule_id)
            if rule_data is not None:
                rule = ConformanceRule.model_validate(rule_data).with_rule_id(rule_id)
                # Always include the rule - let the converter handle applicability filtering with SkippedNonApplicableCheck
                relevant_rules[rule_id] = rule
                rule_dependencies = relevant_rules[rule_id].validation_criteria.dependencies
                for rule_dep in rule_dependencies:
                    if rule_dep not in relevant_rules:
                        dependencies.append(rule_dep)
            else:
                self.log.warning("Rule ID %s listed in dataset but not found in raw rules data", rule_id)

        processed_deps = set()
        while dependencies:
            dep_id = dependencies.popleft()
            if dep_id in processed_deps:
                continue
            processed_deps.add(dep_id)

            dep_rule_data = raw_rules_data.get(dep_id)
            if dep_rule_data is not None and dep_id not in relevant_rules:
                dep_rule = ConformanceRule.model_validate(dep_rule_data).with_rule_id(dep_id)
                # Always include the dependency rule - let the converter handle applicability filtering with SkippedNonApplicableCheck
                relevant_rules[dep_id] = dep_rule
                rule_dependencies = relevant_rules[dep_id].validation_criteria.dependencies
                for rule_dep in rule_dependencies:
                    if rule_dep not in relevant_rules and rule_dep not in processed_deps:
                        dependencies.append(rule_dep)
            elif dep_rule_data is None:
                self.log.warning("Dependency Rule ID %s not found in raw rules data", dep_id)
        
        # Propagate composite conditions into each referenced rule's private attr
        self._propagate_composite_conditions(relevant_rules)

        return relevant_rules

    def buildDependencyGraph(self, target_rule_prefix: Optional[str] = "BilledCost") -> None:
        """
        Build dependency graph for rules with the given prefix.
        If target_rule_prefix is None, processes all rules.
        Otherwise, processes rules that start with target_rule_prefix and their dependencies.
        """
        # Filter rules by prefix first (or use all rules if prefix is None)
        filtered_rules = self.rules
        if target_rule_prefix is not None:
            # Start with rules matching the prefix
            initial_rules = {
                rule_id: rule
                for rule_id, rule in self.rules.items()
                if rule_id.startswith(target_rule_prefix)
            }

            # Collect all dependencies recursively to include child rules of composite rules
            filtered_rules = self._collectAllDependencies(initial_rules)

        # Build graph for filtered rules
        for rule_id, rule in filtered_rules.items():
            dependencies = rule.validation_criteria.dependencies

            # Only add dependencies that are also in our filtered set
            filtered_dependencies = {
                dep for dep in dependencies
                if dep in filtered_rules
            }

            self.dependency_graph[rule_id] = filtered_dependencies
            self.in_degree[rule_id] = len(filtered_dependencies)

            # Build reverse graph
            for dependency in filtered_dependencies:
                self.reverse_graph[dependency].append(rule_id)

    def _collectAllDependencies(self, initial_rules: Dict[str, Any]) -> Dict[str, Any]:
        """
        Recursively collect all dependencies of the initial rules to include
        child rules of composite rules that may not match the prefix filter.
        """
        all_rules = initial_rules.copy()
        to_process = deque(initial_rules.keys())
        processed = set()

        while to_process:
            current_rule_id = to_process.popleft()
            if current_rule_id in processed:
                continue

            processed.add(current_rule_id)

            # Get rule data - check if it exists in our source data
            if current_rule_id not in self.rules:
                continue

            rule = self.rules[current_rule_id]
            dependencies = rule.validation_criteria.dependencies

            # Add dependencies to our collection and queue them for processing
            for dep_id in dependencies:
                if dep_id in self.rules and dep_id not in all_rules:
                    all_rules[dep_id] = self.rules[dep_id]
                    to_process.append(dep_id)

        return all_rules

    def getTopologicalOrder(self) -> List[str]:
        """
        Perform topological sort using Kahn's algorithm.
        Returns rules in order such that dependencies are processed before dependents.
        """
        # Copy in_degree to avoid modifying original
        in_degree_copy = self.in_degree.copy()
        queue = deque()
        result = []

        if log.isEnabledFor(logging.DEBUG) or log.isEnabledFor(logging.WARNING):
            forward_graph = {rule: deps for rule, deps in self.dependency_graph.items()}
            _log_graph_snapshot(forward_graph, name="rule-graph", sample=8)
            _cycles = _log_sccs(forward_graph)
            _log_cycle_details(forward_graph, _cycles)

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

        # Handle circular dependencies by adding remaining rules
        if len(result) != len(self.in_degree):
            remaining_rules = [rule for rule in self.in_degree if rule not in result]
            # Log the circular dependency warning but continue processing
            log.warning("Circular dependency detected among rules: %s", remaining_rules)
            log.info("Adding these rules to the end of the processing order...")
            result.extend(remaining_rules)

        return result

    def getDependencies(self, rule_id: str) -> List[str]:
        """Get direct dependencies for a specific rule."""
        return list(self.dependency_graph.get(rule_id, set()))

    def isCompositeRule(self, rule_id: str) -> bool:
        """
        Check if a rule is composite by examining its Function type
        and whether it has CheckConformanceRule dependencies.
        """
        rule = self.rules.get(rule_id, {})
        has_dependencies = len(rule.validation_criteria.dependencies) > 0
        return rule.function == "Composite" and has_dependencies

    def getCompositeRuleLogic(self, rule_id: str) -> Optional[str]:
        """
        Extract the logic (AND/OR) for a composite rule.
        """
        if rule_id not in self.rules:
            raise ValueError(f"Rule ID '{rule_id}' not found in rules.")

        if not self.isCompositeRule(rule_id):
            return None

        rule = self.rules.get(rule_id)
        return rule.validation_criteria.requirement.get("CheckFunction")

    def _propagate_composite_conditions(
        self,
        rules: Dict[str, "ConformanceRule"],
    ) -> None:
        """
        For every Composite rule with a non-empty ValidationCriteria.condition,
        set that condition as the runtime inherited_precondition on each
        directly referenced rule via CheckConformanceRule.
        """
        for rule in rules.values():
            if rule.function == "Composite" and rule.validation_criteria.condition:
                condition = rule.validation_criteria.condition

                for item in rule.validation_criteria.requirement.get("Items", []):
                    if item.get('CheckFunction', None) == "CheckConformanceRule":
                        dep_rule_id = item.get("ConformanceRuleId", None)
                        if dep_rule_id in rules:
                            rules[dep_rule_id].validation_criteria.precondition = condition
                        else:
                            self.log.warning("Referenced rule ID %s not found for condition propagation", dep_rule_id)

    def getRelevantRules(self) -> Dict[str, "ConformanceRule"]:
        """Return the filtered set of rules relevant to the target prefix and dependencies."""
        return self.rules

    def build_plan_and_schedule(
        self,
        entry_rule_ids: Optional[List[str]] = None,
        *,
        exec_ctx: Optional[dict] = None,
        rules_dict: Optional[Dict[str, any]] = None,
        checkfunctions_dict: Optional[Dict[str, any]] = None,
    ) -> ValidationPlan:
        """
        Build a parent-preserving PlanGraph via recursive expansion, then compile to a
        ValidationPlan (index-based, layered) for fast execution.

        - entry_rule_ids: explicit roots; if None, uses all relevant rules.
        - exec_ctx: optional runtime context for future edge gating.
        - rules_dict/checkfunctions_dict: pass-through of your raw JSON maps so the plan can
          carry everything the executor might need, without extra plumbing.
        """
        relevant_rules = self.getRelevantRules()  # already computed after buildDependencyGraph()

        # Choose roots
        roots = entry_rule_ids if entry_rule_ids else list(relevant_rules.keys())

        # Build plan graph
        builder = PlanBuilder(relevant_rules)
        plan_graph = builder.build_forest(roots)

        # Compile to execution-ready plan
        val_plan = compile_validation_plan(
            plan_graph=plan_graph,
            rules_dict=rules_dict or self.raw_rules_data,         # fall back to what you already hold
            checkfunctions=checkfunctions_dict or {},             # pass through if available
            key_fn=default_key_fn(plan_graph),
            exec_ctx=exec_ctx,
        )

        # Keep for introspection/diagnostics if useful
        self._last_validation_plan = val_plan
        return val_plan
