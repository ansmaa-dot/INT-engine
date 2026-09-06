import json

import pytest

from core.canonical_paths import (
    assign_path,
    resolve_path,
    unflatten_dot_keys,
)


def test_resolve_nested_and_list_index():
    data = {
        "patient": {"name": "Ada", "identifiers": [{"value": "42"}]},
        "observations": [{"value": 1}, {"value": 2}],
    }
    assert resolve_path(data, "patient.name") == "Ada"
    assert resolve_path(data, "patient.identifiers.0.value") == "42"
    assert resolve_path(data, "observations.1.value") == 2
    assert resolve_path(data, "patient.missing") is None
    assert resolve_path(data, "observations.9.x") is None
    assert resolve_path(None, "patient.name") is None


def test_assign_creates_nested_dict():
    target = {}
    assign_path(target, "patient.name", "Ada")
    assert target == {"patient": {"name": "Ada"}}


def test_assign_creates_list_index():
    target = {"observations": []}
    assign_path(target, "observations.0.value", 5)
    assert target["observations"][0]["value"] == 5


def test_assign_extensions():
    target = {}
    assign_path(target, "extensions.vendor.flag", True)
    assert target == {"extensions": {"vendor": {"flag": True}}}


def test_assign_overwrites_existing():
    target = {"patient": {"name": "x"}}
    assign_path(target, "patient.name", "y")
    assert target["patient"]["name"] == "y"


# ── unflatten_dot_keys ──────────────────────────────────────────────


def test_unflatten_flat_dot_keys_into_nested():
    flat = {"patient.name": "Ada", "patient.identifiers.0.value": "42"}
    assert unflatten_dot_keys(flat) == {
        "patient": {"name": "Ada", "identifiers": [{"value": "42"}]},
    }


def test_unflatten_leaves_nested_payload_untouched():
    nested = {"patient": {"name": "Ada"}, "extensions": {"a.b": 1}}
    # dotted keys already nested in a sub-object are NOT top-level; untouched
    assert unflatten_dot_keys(nested) == nested


def test_unflatten_mixed_flat_and_plain_keys():
    assert unflatten_dot_keys({"volume": 1, "a.b.c": 2}) == {
        "volume": 1, "a": {"b": {"c": 2}},
    }


def test_unflatten_returns_payload_instance_when_no_flat_keys():
    payload = {"x": 1}
    assert unflatten_dot_keys(payload) is payload


def test_unflatten_non_dict_passthrough():
    assert unflatten_dot_keys(None) is None
    assert unflatten_dot_keys([1, 2]) == [1, 2]