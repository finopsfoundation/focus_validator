# plan_builder.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Set, Optional, Callable, Tuple, Iterable, Any
from collections import defaultdict
import heapq
import re
from focus_validator.config_objects import ConformanceRule


Predicate = Callable[[dict], bool]

# ---------------------------------------------------------------------------
# Graph construction (recursive plan builder → topo schedule)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class EdgeCtx:
    """Why this dependency exists and (optionally) when it’s active."""
    kind: str  # "structural" | "data_dep" | "applicability" | "ordering"
    note: Optional[str] = None
    predicate: Optional[Predicate] = None  # if provided, edge only counts when predicate(ctx) is True

@dataclass
class PlanNode:
    rule_id: str
    rule: ConformanceRule
    parents: List["PlanNode"] = field(default_factory=list)             # inbound parent nodes
    parent_edges: Dict[str, EdgeCtx] = field(default_factory=dict)      # parent_id -> EdgeCtx

@dataclass
class PlanGraph:
    nodes: Dict[str, PlanNode] = field(default_factory=dict)
    children: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))
    parents: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))
    edges: Dict[Tuple[str, str], EdgeCtx] = field(default_factory=dict)  # (parent, child) -> ctx

    def add_edge(self, parent: str, child: str, ctx: EdgeCtx) -> None:
        if parent == child:
            return
        self.children[parent].add(child)
        self.parents[child].add(parent)
        self.edges[(parent, child)] = ctx

    def topo_schedule(
        self,
        *,
        key_fn: Optional[Callable[[str], Tuple]] = None,
        active_predicate: Optional[Callable[[EdgeCtx, dict], bool]] = None,
        exec_ctx: Optional[dict] = None,
    ) -> Tuple[List[str], List[List[str]]]:
        """
        Kahn-based topo with optional runtime edge gating and deterministic tie-breakers.
        Returns (flat_order, layered_batches_of_rule_ids).
        """
        exec_ctx = exec_ctx or {}
        active_predicate = active_predicate or (lambda ectx, ctx: True)
        key_fn = key_fn or (lambda rid: (rid,))

        indeg = {n: 0 for n in self.nodes}
        for (p, c), ectx in self.edges.items():
            if active_predicate(ectx, exec_ctx):
                indeg[c] = indeg.get(c, 0) + 1
                indeg.setdefault(p, 0)

        heap = [(key_fn(n), n) for n, d in indeg.items() if d == 0]
        heapq.heapify(heap)

        order: List[str] = []
        layers: List[List[str]] = []

        while heap:
            current: List[str] = []
            while heap:
                _, u = heapq.heappop(heap)
                current.append(u)
            current.sort(key=key_fn)
            layers.append(current)
            order.extend(current)

            for u in current:
                for v in self.children.get(u, ()):
                    ectx = self.edges[(u, v)]
                    if not active_predicate(ectx, exec_ctx):
                        continue
                    indeg[v] -= 1
                    if indeg[v] == 0:
                        heapq.heappush(heap, (key_fn(v), v))

        if len(order) != len(self.nodes):
            blocked = [n for n, d in indeg.items() if d > 0]
            raise ValueError(f"Active-edge cycle detected; blocked nodes: {blocked}")
        return order, layers


class PlanBuilder:
    """
    Recursive, memoized builder that expands:
      - structural references (Composite → CheckConformanceRule deps)
      - explicit cross-graph deps (validation_criteria.dependencies)
      - optional applicability gating (`condition`) as edge predicates
    """
    def __init__(self, rules: Dict[str, ConformanceRule]) -> None:
        self.rules = rules
        self.graph = PlanGraph()
        self._memo: Dict[str, PlanNode] = {}

    def build_forest(self, roots: Iterable[str]) -> PlanGraph:
        for rid in roots:
            self._build_node(rid)
        return self.graph

    def _get_or_create(self, rid: str) -> PlanNode:
        if rid in self._memo:
            return self._memo[rid]
        rule = self.rules.get(rid)
        if rule is None:
            raise KeyError(f"Rule {rid} not found")
        node = PlanNode(rule_id=rid, rule=rule)
        self.graph.nodes[rid] = node
        self._memo[rid] = node
        return node

    def _link(self, parent_id: str, child_id: str, ctx: EdgeCtx) -> None:
        parent = self._get_or_create(parent_id)
        child  = self._get_or_create(child_id)
        self.graph.add_edge(parent_id, child_id, ctx)
        child.parents.append(parent)
        child.parent_edges[parent_id] = ctx

    def _build_node(self, rid: str) -> PlanNode:
        if rid in self._memo:
            return self._memo[rid]

        node = self._get_or_create(rid)
        rule = node.rule

        # (1) Structural expansion for Composite rules
        # Adjust these attribute names if your model differs.
        if getattr(rule, "function", None) == "Composite":
            vc = getattr(rule, "validation_criteria", None)
            requirement = getattr(vc, "requirement", {}) if vc else {}
            items = requirement.get("Items", []) or []
            for item in items:
                if item.get("CheckFunction") == "CheckConformanceRule":
                    dep_id = item.get("ConformanceRuleId")
                    if dep_id and dep_id in self.rules:
                        self._build_node(dep_id)
                        self._link(
                            parent_id=dep_id,
                            child_id=rid,
                            ctx=EdgeCtx(kind="structural", note=f"{rid} references {dep_id}")
                        )

        # (2) Cross-graph explicit deps
        vc = getattr(rule, "validation_criteria", None)
        deps = getattr(vc, "dependencies", []) if vc is not None else []
        for dep_id in deps or []:
            if dep_id in self.rules:
                self._build_node(dep_id)
                self._link(
                    parent_id=dep_id,
                    child_id=rid,
                    ctx=EdgeCtx(kind="data_dep", note=f"{rid} depends on {dep_id}")
                )

        # (3) Optional applicability gating at the child side
        condition = getattr(vc, "condition", None) if vc is not None else None
        if condition:
            def _gate(ctx: dict, _cond=condition) -> bool:
                # TODO: replace with your real evaluator.
                # For now treat truthy condition as active.
                return bool(_cond)
            for p in list(self.graph.parents.get(rid, [])):
                k = (p, rid)
                ectx = self.graph.edges[k]
                gated = EdgeCtx(kind=ectx.kind, note=f"{ectx.note}; gated by condition on {rid}", predicate=_gate)
                self.graph.edges[k] = gated
                node.parent_edges[p] = gated

        return node


def default_key_fn(plan: PlanGraph) -> Callable[[str], Tuple]:
    """
    Order policy:
      1) -000- first
      2) EntityType: Dataset (by numeric ID)
      3) EntityType: Column (by numeric ID)
      4) Lexicographic RuleId
    """
    def key(rule_id: str):
        r = plan.nodes[rule_id].rule
        m = re.search(r"-([0-9]{3})-", rule_id)
        num = int(m.group(1)) if m else 999
        etype = getattr(r, "entity_type", None) or getattr(r, "EntityType", "") or ""
        et_ord = {"Dataset": 0, "Column": 1}.get(etype, 2)
        zero_boost = 0 if num == 0 else 1
        return (zero_boost, et_ord, num, rule_id)
    return key

# ---------------------------------------------------------------------------
# Execution-ready compilation (index-based plan for a tight validation loop)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ExecNode:
    rule_id: str
    idx: int
    parent_idxs: Tuple[int, ...]           # indices into ValidationPlan.nodes
    parent_edges: Tuple[EdgeCtx, ...]      # aligned with parent_idxs
    rule: Any                              # keep full rule for generators

@dataclass
class ValidationPlan:
    """
    Everything the executor needs, precomputed and cache-friendly.
    """
    nodes: List[ExecNode]                  # index-addressable nodes in topo order
    id2idx: Dict[str, int]                 # rule_id -> idx
    layers: List[List[int]]                # batches of indices (parallelizable)
    plan_graph: PlanGraph                  # full graph for diagnostics
    rules_dict: Dict[str, Any]             # original rules JSON (if needed)
    checkfunctions: Dict[str, Any]         # original check functions map

def compile_validation_plan(
    *,
    plan_graph: PlanGraph,
    rules_dict: Dict[str, Any],
    checkfunctions: Dict[str, Any],
    key_fn: Optional[Callable[[str], Tuple]] = None,
    active_predicate: Optional[Callable[[EdgeCtx, dict], bool]] = None,
    exec_ctx: Optional[dict] = None,
) -> ValidationPlan:
    """
    Turn a parent-preserving PlanGraph into an index-based ValidationPlan for fast execution.
    """
    key_fn = key_fn or default_key_fn(plan_graph)
    order, layer_rule_ids = plan_graph.topo_schedule(
        key_fn=key_fn,
        active_predicate=active_predicate,
        exec_ctx=exec_ctx,
    )

    id2idx: Dict[str, int] = {rid: i for i, rid in enumerate(order)}
    nodes: List[ExecNode] = [None] * len(order)  # type: ignore

    for rid in order:
        idx = id2idx[rid]
        pg_node = plan_graph.nodes[rid]

        parent_idxs: List[int] = []
        parent_edges: List[EdgeCtx] = []
        # Deterministic parent order for stable builds
        for parent in pg_node.parents:
            pid = parent.rule_id
            parent_idxs.append(id2idx[pid])
            parent_edges.append(pg_node.parent_edges[pid])

        # Ensure parents sorted by idx (usually already true due to topo, but make explicit)
        paired = sorted(zip(parent_idxs, parent_edges), key=lambda t: t[0])
        if paired:
            parent_idxs_tuple, parent_edges_tuple = map(tuple, zip(*paired))
        else:
            parent_idxs_tuple, parent_edges_tuple = tuple(), tuple()

        nodes[idx] = ExecNode(
            rule_id=rid,
            idx=idx,
            parent_idxs=parent_idxs_tuple,
            parent_edges=parent_edges_tuple,
            rule=pg_node.rule,
        )

    # Convert layer rule_ids into layer indices
    index_layers: List[List[int]] = [[id2idx[rid] for rid in layer] for layer in layer_rule_ids]

    return ValidationPlan(
        nodes=nodes,
        id2idx=id2idx,
        layers=index_layers,
        plan_graph=plan_graph,
        rules_dict=rules_dict,
        checkfunctions=checkfunctions,
    )
