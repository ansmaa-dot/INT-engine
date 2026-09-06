"""Pipeline step chain: the ``Step`` dataclass and its factory.

A channel's ``pipeline`` is an ordered list of typed steps interpreted by
``ChannelRunner`` at runtime (see PIPELINE_STEP_CHAIN_PLAN.md, §3):

    enrich     — read-only BatchLookup; repeatable
    transform  — pure FieldMapper; repeatable
    filter     — boolean expression; false → on_fail (dead_letter | discard)
    assert     — boolean expression; false → on_fail (retry | dead_letter)

``Step.impl`` holds the runtime object: a ``BatchLookup`` for enrich, a
``FieldMapper`` for transform, and the raw expression dict for filter/assert.
Deep validation (expressions, identifiers, shared refs) happens in the config
layer at save time; this module stays dependency-light and mechanical.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from nodes.transform.field_mapper import FieldMapper
from nodes.enrichment.batch_lookup import BatchLookup

STEP_TYPES = ("enrich", "transform", "filter", "assert")

# Shared-step kinds (shared_steps.kind column) ↔ runtime step types.
# "mapping" and "enrichment" keep the historical names; filter/assert map 1:1.
STEP_TYPE_TO_SHARED_KIND = {
    "enrich": "enrichment",
    "transform": "mapping",
    "filter": "filter",
    "assert": "assert",
}
SHARED_KIND_TO_STEP_TYPE = {v: k for k, v in STEP_TYPE_TO_SHARED_KIND.items()}
SHARED_KINDS = tuple(SHARED_KIND_TO_STEP_TYPE)  # ("mapping", "enrichment", "filter", "assert")

# on_fail vocabulary per step type (plan §3 D10).
FILTER_ON_FAIL = ("dead_letter", "discard")
ASSERT_ON_FAIL = ("retry", "dead_letter")


@dataclass
class Step:
    step_id: str
    type: str
    config: dict
    on_fail: str | None = None
    impl: Any = None


def build_step(entry: dict) -> Step:
    """Build a ``Step`` from a *resolved* pipeline entry.

    ``entry`` is ``{"step_id": ..., "type": ..., "config": {...}}`` — shared
    references must already be resolved by the config layer. Raises
    ``ValueError`` on structural problems (unknown type, missing keys).
    """
    step_id = entry.get("step_id") or ""
    stype = entry.get("type") or ""
    if stype not in STEP_TYPES:
        raise ValueError(f"unknown step type {stype!r}")

    cfg = entry.get("config") or {}
    if not isinstance(cfg, dict):
        raise ValueError(f"step {step_id!r}: config must be a JSON object")

    on_fail = cfg.get("on_fail")
    if stype == "filter":
        on_fail = on_fail or "dead_letter"
    elif stype == "assert":
        on_fail = on_fail or "dead_letter"

    if stype == "enrich":
        impl: Any = BatchLookup(
            db_path=cfg.get("lookup_db_path"),
            source_key_field=cfg.get("source_key_field", ""),
            target_table=cfg.get("target_table", ""),
            target_key_col=cfg.get("target_key_col", ""),
            fields=cfg.get("fields") or [],
            lookup_name=cfg.get("lookup_name", ""),
        )
    elif stype == "transform":
        impl = FieldMapper(cfg.get("rules") or [])
    else:  # filter / assert
        impl = cfg.get("expression")

    return Step(step_id=step_id, type=stype, config=cfg, on_fail=on_fail, impl=impl)
