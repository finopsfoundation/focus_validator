# ──────────────────────────────────────────────────────────────────────────────
# Edge helpers (as provided)
from typing import Any, Dict, Iterable, Tuple, Optional
from graphviz import Digraph

def _idx2rid_map(plan) -> Dict[int, str]:
    m = {}
    for i, n in enumerate(getattr(plan, "nodes", []) or []):
        rid = getattr(n, "rule_id", None) or getattr(n, "id", None)
        if isinstance(rid, str):
            m[i] = rid
    return m

def _rid2idx_map(plan) -> Dict[str, int]:
    m = getattr(plan, "id2idx", None)
    if isinstance(m, dict):
        return m
    m = {}
    for i, n in enumerate(getattr(plan, "nodes", []) or []):
        rid = getattr(n, "rule_id", None) or getattr(n, "id", None)
        if isinstance(rid, str):
            m[rid] = i
    return m

def _edge_endpoints_from_edgectx(e: Any) -> Optional[Tuple[Any, Any]]:
    # Try indices first
    u = getattr(e, "parent_idx", None) or getattr(e, "src_idx", None) or getattr(e, "u", None)
    v = getattr(e, "child_idx",  None) or getattr(e, "dst_idx", None) or getattr(e, "v", None)
    if u is not None and v is not None:
        return (u, v)
    # Then rule_ids
    ur = (getattr(e, "parent_rule_id", None)
          or getattr(e, "src_rule_id", None)
          or getattr(e, "rule_id", None))
    vr = (getattr(e, "child_rule_id", None)
          or getattr(e, "dst_rule_id", None))
    if ur is not None and vr is not None:
        return (ur, vr)
    return None

def add_plan_edges(g: Digraph, plan, *, use_rule_ids: bool = True) -> None:
    """Add edges to Graphviz, handling EdgeCtx, tuples, dict adjacency, or networkx graphs."""
    rid2idx = _rid2idx_map(plan)
    idx2rid = _idx2rid_map(plan)

    def _name(x):
        # Convert endpoint to the node name you used when adding nodes
        if use_rule_ids:
            if isinstance(x, int):
                return idx2rid.get(x)
            return x
        else:
            if isinstance(x, str):
                i = rid2idx.get(x)
                return None if i is None else str(i)
            return str(x)

    pg = getattr(plan, "plan_graph", None)
    if pg is not None:
        edges_attr = getattr(pg, "edges", None)

        # networkx-like
        if callable(edges_attr):
            edges_result = edges_attr()
            # Handle case where edges_attr() returns a single EdgeCtx instead of an iterable
            if hasattr(edges_result, '__iter__') and not isinstance(edges_result, (str, bytes)):
                edge_iter = edges_result
            else:
                # If it's a single EdgeCtx object, wrap it in a list
                edge_iter = [edges_result] if edges_result is not None else []
                
            for e in edge_iter:          # e might be tuple OR EdgeCtx
                if isinstance(e, (tuple, list)) and len(e) == 2:
                    u, v = e
                else:
                    pair = _edge_endpoints_from_edgectx(e)
                    if not pair:
                        continue
                    u, v = pair
                nu, nv = _name(u), _name(v)
                if nu is not None and nv is not None:
                    g.edge(nu, nv)
            return

        # Handle PlanGraph.edges format: {(parent, child): EdgeCtx}
        if isinstance(edges_attr, dict):
            # Check if this is PlanGraph.edges format (keys are tuples, values are EdgeCtx)
            first_key = next(iter(edges_attr.keys()), None) if edges_attr else None
            if isinstance(first_key, tuple) and len(first_key) == 2:
                # PlanGraph.edges format: {(parent_id, child_id): EdgeCtx}
                for (u, v), edge_ctx in edges_attr.items():
                    nu, nv = _name(u), _name(v)
                    if nu is not None and nv is not None:
                        g.edge(nu, nv)
                return
            else:
                # Standard dict adjacency: {u: [v1, v2, ...]}
                for u, vs in edges_attr.items():
                    # Handle case where vs is a single EdgeCtx instead of an iterable
                    if hasattr(vs, '__iter__') and not isinstance(vs, (str, bytes)):
                        v_iter = vs
                    else:
                        # If it's a single EdgeCtx object, wrap it in a list
                        v_iter = [vs]
                        
                    for v in v_iter:
                        nu, nv = _name(u), _name(v)
                        if nu is not None and nv is not None:
                            g.edge(nu, nv)
                return

        # iterable of EdgeCtx / tuples
        if edges_attr is not None:
            # Handle case where edges_attr is a single EdgeCtx instead of an iterable
            if hasattr(edges_attr, '__iter__') and not isinstance(edges_attr, (str, bytes)):
                edge_iter = edges_attr
            else:
                # If it's a single EdgeCtx object, wrap it in a list
                edge_iter = [edges_attr]
                
            for e in edge_iter:
                if isinstance(e, (tuple, list)) and len(e) == 2:
                    u, v = e
                else:
                    pair = _edge_endpoints_from_edgectx(e)
                    if not pair:
                        continue
                    u, v = pair
                nu, nv = _name(u), _name(v)
                if nu is not None and nv is not None:
                    g.edge(nu, nv)
            return

    # Fallback: plan.edges
    pedges = getattr(plan, "edges", None)
    if pedges:
        # Handle case where pedges is a single EdgeCtx instead of an iterable
        if hasattr(pedges, '__iter__') and not isinstance(pedges, (str, bytes)):
            edge_iter = pedges
        else:
            # If it's a single EdgeCtx object, wrap it in a list
            edge_iter = [pedges]
            
        for e in edge_iter:
            if isinstance(e, (tuple, list)) and len(e) == 2:
                u, v = e
            else:
                pair = _edge_endpoints_from_edgectx(e)
                if not pair:
                    continue
                u, v = pair
            nu, nv = _name(u), _name(v)
            if nu is not None and nv is not None:
                g.edge(nu, nv)
        return

    # Last resort: derive from per-node parent collections
    for v_idx, n in enumerate(getattr(plan, "nodes", []) or []):
        for attr in ("parent_indices", "parents_by_idx", "parents", "in_edges"):
            arr = getattr(n, attr, None)
            if not isinstance(arr, (list, tuple, set)):
                continue
            for e in arr:
                if isinstance(e, int):
                    u = e
                elif isinstance(e, (tuple, list)) and len(e) == 2:
                    u, _ = e
                else:
                    pair = _edge_endpoints_from_edgectx(e)
                    u = pair[0] if pair else None
                if u is None:
                    continue
                nu, nv = _name(u), _name(v_idx)
                if nu is not None and nv is not None:
                    g.edge(nu, nv)

# ──────────────────────────────────────────────────────────────────────────────
# Status / shape helpers

COLOR_MAP = {
    "PASSED":  "lightgreen",
    "FAILED":  "lightcoral",
    "ERRORED": "orange",
    "ERRORED*":"orange",
    "SKIPPED": "lightgray",
    "PENDING": "lightyellow",
}
DEFAULT_COLOR = "white"

def _status_from_entry(entry: Dict[str, Any]) -> str:
    ok = bool(entry.get("ok", False))
    d = entry.get("details", {}) or {}
    if d.get("skipped"):
        return "SKIPPED"
    
    # Check if this is a column presence check - treat as business rule failure, not technical error
    is_column_presence = False
    
    # First check if we have explicit check type information
    check_type = d.get("check_type") or d.get("checkType") or ""
    if check_type == "column_presence":
        is_column_presence = True
    elif "error" in d:
        # Fallback: pattern matching in error messages for column presence checks
        error_msg = str(d.get("error", "")).lower()
        message = str(d.get("message", "")).lower()
        combined_text = f"{error_msg} {message}"
        
        if ("column" in combined_text and 
            any(phrase in combined_text for phrase in ["must be present", "not found", "does not exist"])):
            is_column_presence = True
    
    # For column presence checks, treat missing columns as FAILED instead of ERRORED
    if is_column_presence:
        return "PASSED" if ok else "FAILED"
    
    # For other checks, missing columns or errors are technical issues (ERRORED)
    if "error" in d or "missing_columns" in d:
        return "ERRORED" if not ok else "ERRORED*"
    
    return "PASSED" if ok else "FAILED"

def _pick_shape(rule: Any, sql_map: Optional[Dict[str, Any]], rid: str) -> str:
    # prefer generator name from sql_map
    meta = None
    if sql_map:
        meta = sql_map.get(rid, {}).get("meta") or sql_map.get(rid, {})
    gen = (meta or {}).get("generator")

    if gen:
        if "ColumnPresent" in gen or "ColumnPresence" in gen:
            return "box"
        if "Format" in gen or "Regex" in gen:
            return "diamond"
        if "Composite" in gen:
            return "ellipse"
        if "Skipped" in gen:
            return "ellipse"

    # fallback: try requirement.CheckFunction
    vc = getattr(rule, "validation_criteria", None) or getattr(rule, "ValidationCriteria", None)
    requirement = vc.get("Requirement") if isinstance(vc, dict) else getattr(vc, "Requirement", None)
    if isinstance(requirement, dict):
        fn = (requirement.get("CheckFunction") or "").lower()
        if "column" in fn and "present" in fn:
            return "box"
        if "regex" in fn or "format" in fn:
            return "diamond"

    # condition-only nodes (rare)
    has_req = bool(requirement)
    has_cond = bool((vc or {}).get("Condition") if isinstance(vc, dict) else getattr(vc, "Condition", None))
    if has_cond and not has_req:
        return "hexagon"

    return "ellipse"

# ──────────────────────────────────────────────────────────────────────────────
# The function you asked for

def build_validation_graph(
    plan,
    results,  # ValidationResults
    *,
    sql_map: Optional[Dict[str, Any]] = None,
    use_rule_ids: bool = True,  # set False if you create nodes by index
    graph_attr: Optional[Dict[str, str]] = None,
    node_attr: Optional[Dict[str, str]] = None,
    edge_attr: Optional[Dict[str, str]] = None,
) -> Digraph:
    """
    Build a Graphviz Digraph with nodes colored by validation status and edges from plan.
    - If use_rule_ids=True, node names are rule_ids and edges use rule_ids directly.
    - If use_rule_ids=False, node names are indices (str(idx)) and edges are mapped via plan.id2idx.
    """
    g = Digraph("focus_validation", format="svg")

    # defaults
    g.graph_attr.update({"rankdir": "LR", "fontsize": "10", "labelloc": "t", "label": "FOCUS Validation Plan"})
    g.node_attr.update({"style": "filled", "fontsize": "9", "fontname": "Helvetica"})
    g.edge_attr.update({"arrowsize": "0.7"})
    if graph_attr: g.graph_attr.update(graph_attr)
    if node_attr:  g.node_attr.update(node_attr)
    if edge_attr:  g.edge_attr.update(edge_attr)

    nodes = getattr(plan, "nodes", []) or []

    # Draw nodes
    for idx, node in enumerate(nodes):
        rid  = getattr(node, "rule_id", None) or getattr(node, "id", None) or f"rule_{idx}"
        rule = getattr(node, "rule", None)

        entry = (results.by_rule_id.get(rid) if use_rule_ids else results.by_idx.get(idx)) or {}
        d = entry.get("details", {}) or {}
        status = _status_from_entry(entry)
        color  = COLOR_MAP.get(status, DEFAULT_COLOR)
        shape  = _pick_shape(rule, sql_map, rid)

        v = d.get("violations")
        msg = (d.get("message") or d.get("reason") or "")
        if msg and len(msg) > 120:
            msg = msg[:117] + "…"
        label = f"{rid}\n{status}" + (f"\nviolations={v}" if v is not None else "")

        name = rid if use_rule_ids else str(idx)
        g.node(name=name, label=label, shape=shape, fillcolor=color, tooltip=msg)

    # Draw edges (robust)
    add_plan_edges(g, plan, use_rule_ids=use_rule_ids)

    return g
