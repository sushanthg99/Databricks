"""
security.py
===========
Resolve TM1-style group security into a flat per-user scope table that a
Databricks row filter can join against.

Inputs
------
users    : rows of security_user_group   (Username, Group_Name, Email, Is_Active, ...)
grants   : rows of security_dimension_access
           (Group_Name, Dimension, Element, Access, Element_Type,
            Applies_To_Descendants, Granted_Date, Expiry_Date)
leaf_map : {(dimension, member): {leaf members}}   (hierarchy.build_leaf_map)
leaves_by_dim : {dimension: {all leaf members}}

Resolution rules (documented in docs/tm1_pipeline.md)
-----------------------------------------------------
1. Inactive users get no scope (they see nothing).
2. A grant is effective when Granted_Date <= today and (no Expiry_Date or
   Expiry_Date >= today).
3. Access READ / WRITE / LOCK are positive grants (LOCK = read-only);
   NONE is an explicit denial.
4. A grant on a Consolidated / Root / Alternate Consolidated element, or with
   Applies_To_Descendants = 'Y', expands to all leaves below it.
5. Per user and dimension:
     positives present -> allowed = positives - denied
     only denials      -> allowed = every leaf of the dimension - denied
     neither           -> dimension is open: one row with member '*'
   Explicit NONE always wins over a grant.
6. A user with no effective grants at all is denied everywhere (fail closed).

Output row: identity, username, dimension, member, access ('READ'|'WRITE').

Pure Python - no Spark dependency.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Iterable, Mapping

POSITIVE = {"READ", "WRITE", "LOCK"}
CONSOLIDATED_TYPES = {"Consolidated", "Root", "Alternate Consolidated"}
OPEN_MEMBER = "*"


@dataclass
class ResolveReport:
    inactive_users: list[str] = field(default_factory=list)
    users_without_grants: list[str] = field(default_factory=list)
    expired_grants: int = 0
    not_yet_effective_grants: int = 0
    unknown_elements: list[tuple[str, str]] = field(default_factory=list)


def parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    s = str(value).strip()
    return date.fromisoformat(s[:10]) if s else None


def is_effective(grant: Mapping[str, Any], today: date) -> tuple[bool, str]:
    granted, expiry = parse_date(grant.get("Granted_Date")), parse_date(grant.get("Expiry_Date"))
    if granted and granted > today:
        return False, "future"
    if expiry and expiry < today:
        return False, "expired"
    return True, ""


def expand_element(
    dimension: str, element: str, element_type: str | None,
    applies_to_descendants: str | None,
    leaf_map: Mapping[tuple[str, str], set[str]],
    report: ResolveReport | None = None,
) -> set[str]:
    leaves = leaf_map.get((dimension, element))
    if leaves is None:
        if report is not None:
            report.unknown_elements.append((dimension, element))
        return {element}
    wants_children = (element_type in CONSOLIDATED_TYPES) or (applies_to_descendants == "Y")
    return set(leaves) if wants_children else {element}


def _identity(username: str, email: str | None, identity_map: Mapping[str, str]) -> str:
    if username in identity_map:
        return identity_map[username].lower()
    return (email or username).strip().lower()


def resolve_scopes(
    users: Iterable[Mapping[str, Any]],
    grants: Iterable[Mapping[str, Any]],
    leaf_map: Mapping[tuple[str, str], set[str]],
    leaves_by_dim: Mapping[str, set[str]],
    secured_dimensions: Iterable[str],
    today: date | None = None,
    identity_map: Mapping[str, str] | None = None,
) -> tuple[list[dict[str, str]], ResolveReport]:
    today = today or date.today()
    identity_map = identity_map or {}
    secured_dimensions = list(secured_dimensions)
    report = ResolveReport()

    # ---- group -> effective grants -------------------------------------
    by_group: dict[str, list[Mapping[str, Any]]] = {}
    for g in grants:
        ok, why = is_effective(g, today)
        if not ok:
            if why == "expired":
                report.expired_grants += 1
            else:
                report.not_yet_effective_grants += 1
            continue
        by_group.setdefault(g["Group_Name"], []).append(g)

    # ---- user -> groups / email ----------------------------------------
    groups: dict[str, set[str]] = {}
    emails: dict[str, str] = {}
    active: dict[str, bool] = {}
    for u in users:
        name = u["Username"]
        if str(u.get("Is_Active", "Y")).upper() != "Y":
            active.setdefault(name, False)
            continue
        active[name] = True
        groups.setdefault(name, set()).add(u["Group_Name"])
        if u.get("Email") and name not in emails:
            emails[name] = u["Email"]

    report.inactive_users = sorted(n for n, a in active.items() if not a and n not in groups)

    rows: list[dict[str, str]] = []
    for username, grp in sorted(groups.items()):
        user_grants = [g for gn in grp for g in by_group.get(gn, [])]
        if not user_grants:
            report.users_without_grants.append(username)
            continue
        identity = _identity(username, emails.get(username), identity_map)

        for dim in secured_dimensions:
            positive: dict[str, str] = {}
            denied: set[str] = set()
            for g in user_grants:
                if g["Dimension"] != dim:
                    continue
                members = expand_element(dim, g["Element"], g.get("Element_Type"),
                                         g.get("Applies_To_Descendants"), leaf_map, report)
                access = str(g["Access"]).upper()
                if access == "NONE":
                    denied |= members
                elif access in POSITIVE:
                    level = "WRITE" if access == "WRITE" else "READ"
                    for m in members:
                        if positive.get(m) != "WRITE":
                            positive[m] = level

            if positive:
                allowed = {m: a for m, a in positive.items() if m not in denied}
            elif denied:
                allowed = {m: "READ" for m in leaves_by_dim.get(dim, set()) if m not in denied}
            else:
                rows.append({"identity": identity, "username": username, "dimension": dim,
                             "member": OPEN_MEMBER, "access": "READ"})
                continue
            for m, a in sorted(allowed.items()):
                rows.append({"identity": identity, "username": username, "dimension": dim,
                             "member": m, "access": a})
    return rows, report
