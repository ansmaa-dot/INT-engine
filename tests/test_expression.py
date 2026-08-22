"""Tests for the expression engine (core/expression.py)."""
import pytest

from core.expression import (
    ExpressionError,
    KNOWN_OPS,
    evaluate,
    validate_expression,
)


# ── helpers ────────────────────────────────────────────────────

def _cat(*paths: str) -> frozenset[str]:
    """Quick catalog builder for tests."""
    return frozenset(paths)


# ── evaluate: value access ─────────────────────────────────────

def test_var_resolves_dotted_path():
    e = {"var": ["patient.name"]}
    assert evaluate(e, {"patient": {"name": "Ada"}}) == "Ada"


def test_var_returns_none_for_missing_path():
    e = {"var": ["patient.name"]}
    assert evaluate(e, {}) is None


def test_var_returns_none_for_missing_intermediate():
    e = {"var": ["patient.name"]}
    assert evaluate(e, {"patient": None}) is None


def test_var_supports_list_index():
    e = {"var": ["ids.0"]}
    assert evaluate(e, {"ids": ["a", "b"]}) == "a"


def test_literal():
    assert evaluate({"literal": ["hello"]}, {}) == "hello"
    assert evaluate({"literal": [42]}, {}) == 42
    assert evaluate({"literal": [None]}, {}) is None


# ── evaluate: comparisons ──────────────────────────────────────

def test_eq():
    assert evaluate({"==": [{"literal": [1]}, {"literal": [1]}]}, {}) is True
    assert evaluate({"==": [{"literal": [1]}, {"literal": [2]}]}, {}) is False


def test_neq():
    assert evaluate({"!=": [{"literal": [1]}, {"literal": [2]}]}, {}) is True


def test_gt_lt():
    assert evaluate({">": [{"literal": [5]}, {"literal": [3]}]}, {}) is True
    assert evaluate({"<": [{"literal": [3]}, {"literal": [5]}]}, {}) is True


def test_comparison_none_returns_false():
    """Comparisons with None against a value return False.
    
    ``!=`` with None is the exception — None != 1 is True, which is 
    correct Python behavior.  The ``_comparison_op`` guard only catches
    ``==``, ``>``, ``>=``, ``<``, ``<=`` — not ``!=``.
    """
    assert evaluate({"==": [{"var": ["nonexistent"]}, {"literal": [1]}]}, {}) is False
    assert evaluate({"!=": [{"var": ["nonexistent"]}, {"literal": [1]}]}, {}) is True
    assert evaluate({">": [{"var": ["nonexistent"]}, {"literal": [1]}]}, {}) is False
    assert evaluate({"<": [{"var": ["nonexistent"]}, {"literal": [1]}]}, {}) is False


# ── evaluate: membership / substring ───────────────────────────

def test_in_list():
    e = {"in": [{"literal": [2]}, {"literal": [[1, 2, 3]]}]}
    assert evaluate(e, {}) is True


def test_in_list_false():
    e = {"in": [{"literal": [9]}, {"literal": [[1, 2, 3]]}]}
    assert evaluate(e, {}) is False


def test_in_non_list_returns_false():
    e = {"in": [{"literal": [2]}, {"literal": ["notalist"]}]}
    assert evaluate(e, {}) is False


def test_contains_substring():
    e = {"contains": [{"literal": ["hello world"]}, {"literal": ["world"]}]}
    assert evaluate(e, {}) is True


def test_contains_non_string_returns_false():
    e = {"contains": [{"literal": [42]}, {"literal": [2]}]}
    assert evaluate(e, {}) is False


# ── evaluate: logic ────────────────────────────────────────────

def test_and_all_true():
    e = {"and": [{"literal": [True]}, {"literal": [True]}]}
    assert evaluate(e, {}) is True


def test_and_one_false():
    e = {"and": [{"literal": [True]}, {"literal": [False]}]}
    assert evaluate(e, {}) is False


def test_or_one_true():
    e = {"or": [{"literal": [False]}, {"literal": [True]}]}
    assert evaluate(e, {}) is True


def test_or_all_false():
    e = {"or": [{"literal": [False]}, {"literal": [False]}]}
    assert evaluate(e, {}) is False


def test_not():
    e = {"not": [{"literal": [False]}]}
    assert evaluate(e, {}) is True


def test_if_then():
    e = {"if": [{"literal": [True]}, {"literal": ["yes"]}, {"literal": ["no"]}]}
    assert evaluate(e, {}) == "yes"


def test_if_else():
    e = {"if": [{"literal": [False]}, {"literal": ["yes"]}, {"literal": ["no"]}]}
    assert evaluate(e, {}) == "no"


def test_if_no_else():
    e = {"if": [{"literal": [False]}, {"literal": ["yes"]}]}
    assert evaluate(e, {}) is None


# ── evaluate: existence ────────────────────────────────────────

def test_exists():
    assert evaluate({"exists": [{"literal": [1]}]}, {}) is True
    assert evaluate({"exists": [{"var": ["nope"]}]}, {}) is False


def test_missing():
    assert evaluate({"missing": [{"var": ["nope"]}]}, {}) is True
    assert evaluate({"missing": [{"literal": [1]}]}, {}) is False


def test_empty():
    assert evaluate({"empty": [{"literal": [""]}]}, {}) is True
    assert evaluate({"empty": [{"literal": [[]]}]}, {}) is True
    assert evaluate({"empty": [{"literal": [{}]}]}, {}) is True
    assert evaluate({"empty": [{"literal": [0]}]}, {}) is True
    assert evaluate({"empty": [{"var": ["nope"]}]}, {}) is True
    assert evaluate({"empty": [{"literal": ["hi"]}]}, {}) is False


def test_not_empty():
    assert evaluate({"not_empty": [{"literal": ["hi"]}]}, {}) is True
    assert evaluate({"not_empty": [{"literal": [""]}]}, {}) is False


def test_truthy():
    assert evaluate({"truthy": [{"literal": ["hi"]}]}, {}) is True
    assert evaluate({"truthy": [{"literal": [True]}]}, {}) is True
    assert evaluate({"truthy": [{"literal": [1]}]}, {}) is True
    assert evaluate({"truthy": [{"literal": [None]}]}, {}) is False
    assert evaluate({"truthy": [{"literal": [False]}]}, {}) is False
    assert evaluate({"truthy": [{"literal": [0]}]}, {}) is False
    assert evaluate({"truthy": [{"literal": [""]}]}, {}) is False
    assert evaluate({"truthy": [{"literal": [[]]}]}, {}) is False
# ── evaluate: error cases ──────────────────────────────────────

def test_unknown_operator_raises():
    e = {"frobnicate": [1, 2]}
    with pytest.raises(ExpressionError, match="unknown operator"):
        evaluate(e, {})


def test_args_not_a_list_raises():
    e = {"==": "notalist"}
    with pytest.raises(ExpressionError, match="must be a list"):
        evaluate(e, {})


def test_multi_key_dict_raises():
    e = {"==": [1, 2], "!=": [3, 4]}
    with pytest.raises(ExpressionError, match="exactly one key"):
        evaluate(e, {})


def test_var_wrong_arg_count():
    with pytest.raises(ExpressionError, match="exactly one string argument"):
        evaluate({"var": []}, {})
    with pytest.raises(ExpressionError, match="exactly one string argument"):
        evaluate({"var": ["a", "b"]}, {})


# ── evaluate: complex / nested expressions ─────────────────────

def test_nested_and_or():
    """(a == 1 and b == 2) or (c == 3)"""
    e = {
        "or": [
            {
                "and": [
                    {"==": [{"var": ["a"]}, {"literal": [1]}]},
                    {"==": [{"var": ["b"]}, {"literal": [2]}]},
                ]
            },
            {"==": [{"var": ["c"]}, {"literal": [3]}]},
        ]
    }
    assert evaluate(e, {"a": 1, "b": 2, "c": 0}) is True  # first branch true
    assert evaluate(e, {"a": 0, "b": 2, "c": 3}) is True  # second branch true
    assert evaluate(e, {"a": 0, "b": 0, "c": 0}) is False


def test_example_assert_result_value_present():
    """From the plan: assert result value present.

    ``not_empty`` of ``None`` is False because ``empty`` treats None as
    empty.  The expression catches both missing keys and None values
    — both signal \"no result\".
    """
    e = {"not_empty": [{"var": ["observations.0.value"]}]}
    assert evaluate(e, {"observations": [{"value": "positive"}]}) is True
    assert evaluate(e, {"observations": [{"value": None}]}) is False  # None is empty
    assert evaluate(e, {"observations": [{}]}) is False  # missing value


def test_leaf_values_passthrough():
    """Non-dict values evaluate to themselves."""
    assert evaluate(True, {}) is True
    assert evaluate(False, {}) is False
    assert evaluate("hello", {}) == "hello"
    assert evaluate(42, {}) == 42
    assert evaluate(None, {}) is None


def test_empty_dict_is_false():
    assert evaluate({}, {}) is False


# ── validate_expression ────────────────────────────────────────

def test_validate_valid_expression():
    e = {"not_empty": [{"var": ["patient.name"]}]}
    cat = _cat("patient.name", "observations.0.value")
    assert validate_expression(e, cat) == []


def test_validate_unknown_operator():
    e = {"frob": [1]}
    errs = validate_expression(e, _cat())
    assert len(errs) == 1
    assert "unknown operator" in errs[0]


def test_validate_unknown_var_path():
    e = {"==": [{"var": ["patient.nope"]}, {"literal": [1]}]}
    errs = validate_expression(e, _cat("patient.name"))
    assert len(errs) >= 1
    assert any("unknown canonical field" in err for err in errs)


def test_validate_known_paths_all_pass():
    e = {"==": [{"var": ["a"]}, {"var": ["b"]}]}
    assert validate_expression(e, _cat("a", "b")) == []


def test_validate_list_index_canonicalization():
    """Validate accepts path with different index (canonicalizes to 0)."""
    e = {"exists": [{"var": ["patient.identifiers.3.value"]}]}
    assert validate_expression(e, _cat("patient.identifiers.0.value")) == []


def test_validate_wrong_arg_count():
    e = {"==": [{"var": ["x"]}]}  # needs 2 args
    errs = validate_expression(e, _cat("x", "y"))
    assert any("exactly two arguments" in err for err in errs)


def test_validate_var_wrong_arg_type():
    e = {"var": [42]}  # not a string
    errs = validate_expression(e, _cat())
    assert any("exactly one string argument" in err for err in errs)


def test_validate_unknown_var_in_nested_expr():
    e = {
        "or": [
            {"==": [{"var": ["known"]}, {"literal": [1]}]},
            {"==": [{"var": ["unknown"]}, {"literal": [2]}]},
        ]
    }
    errs = validate_expression(e, _cat("known"))
    assert len(errs) >= 1
    assert any("unknown" in err for err in errs)


def test_validate_expands_argument_type_errors():
    e = {"==": "notalist"}
    errs = validate_expression(e, _cat())
    assert any("must be a list" in err for err in errs)


def test_validate_empty_catalog_accepts_all():
    """When catalog is empty, no path checks are done (allow all)."""
    e = {"==": [{"var": ["anything.here"]}, {"literal": [1]}]}
    assert validate_expression(e, _cat()) == []


# ── security: no eval/exec ─────────────────────────────────────

def test_no_eval_in_source():
    """Verify 'eval' and 'exec' are not present in the expression module."""
    import inspect
    src = inspect.getsource(evaluate)
    assert "eval(" not in src
    assert "exec(" not in src


def test_known_ops_has_expected_count():
    """19 operators — locked set."""
    assert len(KNOWN_OPS) == 19