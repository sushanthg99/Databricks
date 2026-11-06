"""
hierarchy.py
============
Flatten TM1 hierarchy JSON (nested ``children``) into relational shapes:

* ``flatten_hierarchies``  -> edge rows  (one per member per hierarchy)
* ``build_closure``        -> closure rows (member, ancestor, distance)
* ``build_leaf_map``       -> {(dimension, member): {leaf members}} across ALL hierarchies

Pure Python - no Spark dependency.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable


def load_hierarchies(path: str | Path) -> list[dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh).get("hierarchies", [])


def _edge(dim, hname, kind, member, name, parent, etype, depth, level, weight, idx) -> dict[str, Any]:
    return {
        "dimension": dim, "hierarchy_name": hname, "hierarchy_kind": kind,
        "member": member, "member_name": name, "parent": parent,
        "element_type": etype, "depth": depth, "level": level,
        "weight": weight, "element_index": idx,
    }


def flatten_hierarchies(
    hierarchies: Iterable[dict[str, Any]], hierarchy_kind: str
) -> list[dict[str, Any]]:
    """One edge row per node. ``hierarchy_kind`` = 'primary' | 'alternate'."""
    rows: list[dict[str, Any]] = []
    for h in hierarchies:
        dim, hname = h["dimension"], h["hierarchy_name"]
        rows.append(_edge(dim, hname, hierarchy_kind, h["root"], h.get("root_name"),
                          None, "Root", 0, h.get("root_level"), None, None))
        stack = [(c, h["root"]) for c in reversed(h.get("children", []))]
        while stack:
            node, parent = stack.pop()
            rows.append(_edge(dim, hname, hierarchy_kind, node["member"], node.get("member_name"),
                              node.get("parent", parent), node.get("element_type"),
                              node.get("depth"), node.get("level"), node.get("weight"),
                              node.get("element_index")))
            for c in reversed(node.get("children", [])):
                stack.append((c, node["member"]))
    return rows


def build_closure(edges: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """(dimension, hierarchy_name, ancestor, member, distance), incl. self at distance 0."""
    parent_of: dict[tuple[str, str, str], str | None] = {}
    for e in edges:
        # A member repeated under two parents in one hierarchy: first wins.
        parent_of.setdefault((e["dimension"], e["hierarchy_name"], e["member"]), e["parent"])
    out: list[dict[str, Any]] = []
    for (dim, hname, member) in parent_of:
        cur, dist, seen = member, 0, set()
        while cur is not None and cur not in seen:
            seen.add(cur)
            out.append({"dimension": dim, "hierarchy_name": hname,
                        "ancestor": cur, "member": member, "distance": dist})
            cur = parent_of.get((dim, hname, cur))
            dist += 1
    return out


def build_leaf_map(edges: Iterable[dict[str, Any]]) -> dict[tuple[str, str], set[str]]:
    """
    {(dimension, member): leaves beneath it}, unioned across every hierarchy.
    A leaf maps to itself so callers need no special case.
    """
    children: dict[tuple[str, str, str], list[str]] = {}
    nodes: set[tuple[str, str, str]] = set()
    for e in edges:
        nodes.add((e["dimension"], e["hierarchy_name"], e["member"]))
        if e["parent"] is not None:
            children.setdefault((e["dimension"], e["hierarchy_name"], e["parent"]), []).append(e["member"])

    memo: dict[tuple[str, str, str], frozenset[str]] = {}

    def leaves(key: tuple[str, str, str], path: frozenset[str]) -> frozenset[str]:
        if key in memo:
            return memo[key]
        kids = children.get(key)
        if not kids:
            res = frozenset({key[2]})
        else:
            acc: set[str] = set()
            for k in kids:
                if k in path:  # cycle guard
                    continue
                acc |= leaves((key[0], key[1], k), path | {k})
            res = frozenset(acc)
        memo[key] = res
        return res

    leaf_map: dict[tuple[str, str], set[str]] = {}
    for key in nodes:
        leaf_map.setdefault((key[0], key[2]), set()).update(leaves(key, frozenset({key[2]})))
    return leaf_map
