"""Canonical path resolution for the canonical model.

Canonical paths are the normal transformation contract. Examples:

    "patient.name"
    "patient.identifiers.0.value"
    "order.accession.value"
    "observations.0.code.value"
    "extensions.vendor.someFlag"

Paths may traverse dicts and lists; integer path segments index into lists.
There is intentionally no ``raw.*`` business namespace — raw wire content is
available only for audit/replay/debugging, never as the transformation
interface.
"""
from __future__ import annotations

from typing import Any


def resolve_path(data: Any, dotted: str) -> Any:
    """Resolve a dotted canonical path against ``data`` (dicts/lists)."""
    cur = data
    for part in dotted.split("."):
        if cur is None:
            return None
        if isinstance(cur, dict):
            cur = cur.get(part)
        elif isinstance(cur, list):
            try:
                idx = int(part)
            except ValueError:
                return None
            cur = cur[idx] if -len(cur) <= idx < len(cur) else None
        else:
            return None
    return cur


def assign_path(target: dict, dotted: str, value: Any) -> None:
    """Assign ``value`` at ``dotted`` inside ``target``, creating intermediate
    dicts (or lists, when the next segment is a numeric index) as needed."""
    parts = dotted.split(".")
    if not parts or parts == [""]:
        return
    cur = target
    for i, part in enumerate(parts[:-1]):
        nxt_part = parts[i + 1]
        if isinstance(cur, dict):
            nxt = cur.get(part)
            if not isinstance(nxt, (dict, list)):
                nxt = [] if nxt_part.isdigit() else {}
                cur[part] = nxt
            cur = nxt
        elif isinstance(cur, list):
            idx = int(part)
            while len(cur) <= idx:
                cur.append(None)
            if cur[idx] is None:
                cur[idx] = [] if nxt_part.isdigit() else {}
            cur = cur[idx]
        else:
            return
    if isinstance(cur, dict):
        cur[parts[-1]] = value
    elif isinstance(cur, list):
        idx = int(parts[-1])
        while len(cur) <= idx:
            cur.append(None)
        cur[idx] = value


def unflatten_dot_keys(payload: dict) -> dict:
    """Expand flat dotted keys into a nested tree (dicts/lists).

    Schemaless inbound sources (db rows, generic JSON webhooks) routinely
    emit dot-notation keys::

        {"patient.name": "Ada", "patient.identifiers.0.value": "42"}

    This folds them back into the nested object the canonical model and the
    outbound structured codecs (HL7/FHIR) expect::

        {"patient": {"name": "Ada", "identifiers": [{"value": "42"}]}}

    Keys without a ``.`` pass through verbatim; integer segments become list
    indices (the same convention canonical paths use). Purely structural —
    no schema knowledge, values are never inspected, nothing is evaluated.
    A payload with no dotted keys is returned unchanged.
    """
    if not isinstance(payload, dict):
        return payload
    flat_keys = [k for k in payload if isinstance(k, str) and "." in k]
    if not flat_keys:
        return payload
    out = {}
    for key, value in payload.items():
        if isinstance(key, str) and "." in key:
            assign_path(out, key, value)
        else:
            out[key] = value
    return out
