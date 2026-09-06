"""JSONLogic-style expression engine for filter/assert steps.

Pure-Python, closed operator set, zero ``eval``/``exec``.

Expression format:

    { "operator": [arg, ...] }

Operators (locked whitelist — no registration API):
  Comparisons:  ``== != > >= < <=``  (two-arg)
  Membership:   ``in``               (value in list)
  Substring:    ``contains``         (haystack contains needle)
  Logic:        ``and or not if``
  Existence:    ``exists missing empty not_empty truthy``
  Value access: ``var``              (dotted canonical path)
                ``literal``          (inline constant)

A ``var`` path resolves via ``core.canonical_paths.resolve_path``; if the
path doesn't exist in the data, it returns ``None`` (never raises).
"""
from __future__ import annotations

from typing import Any

from core.canonical_paths import resolve_path


class ExpressionError(Exception):
    """Typed error for invalid expressions (unknown op, validation failure)."""


def _to_bool(v: Any) -> bool:
    """Coerce any value to bool — used by truthy/missing/empty etc."""
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return len(v) > 0
    if isinstance(v, (int, float)):
        return v != 0
    if isinstance(v, (list, dict, tuple)):
        return len(v) > 0
    return True


def _comparison_op(op: str, a: Any, b: Any) -> bool:
    """Perform a comparison; incomparable types return False."""
    try:
        if op == "==":
            return a == b
        if op == "!=":
            return a != b
        if a is None or b is None:
            return False
        if op == ">":
            return a > b
        if op == ">=":
            return a >= b
        if op == "<":
            return a < b
        if op == "<=":
            return a <= b
    except TypeError:
        return False
    return False
def evaluate(expr: Any, data: dict) -> Any:
    """Recursively evaluate a JSONLogic expression against ``data``.

    ``data`` is a dict representing the canonical message (its JSON-dump
    form plus ``lookups``).  Returns the expression result — for filter/
    assert steps this should be a bool, but intermediate nodes may return
    any value.

    Raises ``ExpressionError`` on an unknown operator.
    """
    if not isinstance(expr, dict):
        return expr

    if len(expr) == 0:
        return False

    if len(expr) != 1:
        raise ExpressionError(
            f"expression dict must have exactly one key, got {len(expr)}"
        )

    op, args = next(iter(expr.items()))

    if not isinstance(args, list):
        raise ExpressionError(
            f"arguments for operator {op!r} must be a list, "
            f"got {type(args).__name__}"
        )

    # --- value access ---

    if op == "var":
        if len(args) != 1 or not isinstance(args[0], str):
            raise ExpressionError(
                "var takes exactly one string argument (dotted path)"
            )
        return resolve_path(data, args[0])

    if op == "literal":
        if len(args) != 1:
            raise ExpressionError("literal takes exactly one argument")
        return args[0]

    # --- comparisons ---

    if op in ("==", "!=", ">", ">=", "<", "<="):
        if len(args) != 2:
            raise ExpressionError(
                f"{op} takes exactly two arguments, got {len(args)}"
            )
        a = evaluate(args[0], data)
        b = evaluate(args[1], data)
        return _comparison_op(op, a, b)

    # --- membership ---

    if op == "in":
        if len(args) != 2:
            raise ExpressionError(
                f"in takes exactly two arguments, got {len(args)}"
            )
        value = evaluate(args[0], data)
        container = evaluate(args[1], data)
        if not isinstance(container, (list, tuple)):
            return False
        return value in container

    # --- substring ---

    if op == "contains":
        if len(args) != 2:
            raise ExpressionError(
                f"contains takes exactly two arguments, got {len(args)}"
            )
        haystack = evaluate(args[0], data)
        needle = evaluate(args[1], data)
        if not isinstance(haystack, str) or not isinstance(needle, str):
            return False
        return needle in haystack

    # --- logic ---

    if op == "and":
        for sub in args:
            if not evaluate(sub, data):
                return False
        return True

    if op == "or":
        for sub in args:
            if evaluate(sub, data):
                return True
        return False

    if op == "not":
        if len(args) != 1:
            raise ExpressionError(
                f"not takes exactly one argument, got {len(args)}"
            )
        return not _to_bool(evaluate(args[0], data))

    if op == "if":
        if len(args) < 2 or len(args) > 3:
            raise ExpressionError(
                "if takes 2 or 3 arguments: [condition, then, else]"
            )
        if _to_bool(evaluate(args[0], data)):
            return evaluate(args[1], data)
        if len(args) == 3:
            return evaluate(args[2], data)
        return None

    # --- existence ---

    if op == "exists":
        if len(args) != 1:
            raise ExpressionError(
                f"exists takes exactly one argument, got {len(args)}"
            )
        return evaluate(args[0], data) is not None

    if op == "missing":
        if len(args) != 1:
            raise ExpressionError(
                f"missing takes exactly one argument, got {len(args)}"
            )
        return evaluate(args[0], data) is None

    if op == "empty":
        if len(args) != 1:
            raise ExpressionError(
                f"empty takes exactly one argument, got {len(args)}"
            )
        val = evaluate(args[0], data)
        if val is None:
            return True
        if isinstance(val, (str, list, dict, tuple, set)):
            return len(val) == 0
        if isinstance(val, (int, float)) and val == 0:
            return True
        return False

    if op == "not_empty":
        if len(args) != 1:
            raise ExpressionError(
                f"not_empty takes exactly one argument, got {len(args)}"
            )
        return not evaluate({"empty": args}, data)

    if op == "truthy":
        if len(args) != 1:
            raise ExpressionError(
                f"truthy takes exactly one argument, got {len(args)}"
            )
        return _to_bool(evaluate(args[0], data))

    raise ExpressionError(f"unknown operator: {op!r}")
# ---------------------------------------------------------------------------
# Validation — static walk at save/compile time
# ---------------------------------------------------------------------------

# All known operators (closed set).
KNOWN_OPS: frozenset[str] = frozenset({
    "==", "!=", ">", ">=", "<", "<=",
    "in", "contains",
    "and", "or", "not", "if",
    "exists", "missing", "empty", "not_empty", "truthy",
    "var", "literal",
})


def validate_expression(
    expr: Any,
    field_catalog: frozenset[str],
    allow_prefixes: tuple[str, ...] = (),
) -> list[str]:
    """Statically walk ``expr`` and return a list of error strings.

    ``field_catalog`` is a set of known dotted canonical paths.
    Any ``var`` referencing an unknown path produces an error — unless it
    starts with one of ``allow_prefixes`` (e.g. ``"lookups."`` for the
    runtime-provided enrichment namespace, which cannot be statically known).
    Returns an empty list when the expression is valid.
    """
    errors: list[str] = []

    def _walk(node: Any, path_hint: str = "<root>") -> None:
        if not isinstance(node, dict):
            return

        if len(node) != 1:
            errors.append(
                f"{path_hint}: expression dict must have exactly one key, "
                f"got {len(node)}"
            )
            return

        op, args = next(iter(node.items()))

        if op not in KNOWN_OPS:
            errors.append(f"{path_hint}: unknown operator {op!r}")
            return

        if not isinstance(args, list):
            errors.append(
                f"{path_hint}: arguments for operator {op!r} must be a list"
            )
            return

        # Validate arg counts
        if op in ("==", "!=", ">", ">=", "<", "<=", "in", "contains"):
            if len(args) != 2:
                errors.append(
                    f"{path_hint}: {op} takes exactly two arguments, "
                    f"got {len(args)}"
                )
        elif op in ("exists", "missing", "empty", "not_empty",
                     "truthy", "not"):
            if len(args) != 1:
                errors.append(
                    f"{path_hint}: {op} takes exactly one argument, "
                    f"got {len(args)}"
                )
        elif op == "if":
            if len(args) < 2 or len(args) > 3:
                errors.append(
                    f"{path_hint}: if takes 2 or 3 arguments, "
                    f"got {len(args)}"
                )
        elif op == "var":
            if len(args) != 1 or not isinstance(args[0], str):
                errors.append(
                    f"{path_hint}: var takes exactly one string argument"
                )
            elif field_catalog:
                if (
                    not _path_matches_catalog(args[0], field_catalog)
                    and not args[0].startswith(allow_prefixes)
                ):
                    errors.append(
                        f"{path_hint}: unknown canonical field {args[0]!r}"
                    )
        elif op == "literal":
            if len(args) != 1:
                errors.append(
                    f"{path_hint}: literal takes exactly one argument"
                )

        # Recurse into sub-expressions
        if op not in ("var", "literal"):
            for i, sub in enumerate(args):
                _walk(sub, f"{path_hint}/{op}[{i}]")

    _walk(expr)
    return errors


def _path_matches_catalog(path: str, catalog: frozenset[str]) -> bool:
    """Check whether *path* could match the field catalog.

    For list fields the catalog uses an example index (e.g. ``0``), but a
    user expression might use a different index.  We canonicalize numeric
    segments to ``0`` before checking.
    """
    if path in catalog:
        return True
    # Canonicalize numeric list indices to "0"
    parts = path.split(".")
    canon_parts = []
    for part in parts:
        # supports negative indices too
        stripped = part.lstrip("-")
        if stripped.isdigit():
            canon_parts.append("0")
        else:
            canon_parts.append(part)
    return ".".join(canon_parts) in catalog