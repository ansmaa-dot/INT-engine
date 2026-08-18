import json

import pytest

from core.canonical_paths import assign_path, resolve_path


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