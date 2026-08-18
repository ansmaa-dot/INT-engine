# nodes/transform/field_mapper.py
"""FieldMapper: transforms a CanonicalMessage (as its JSON form) using
canonical paths as the normal contract.

  * sources:  "patient.name", "order.accession.value", "observations.0.value",
              "lookups.<lookup_name>.<field>" for enrichment reference data
  * targets:  dotted canonical paths written back into the message

The mapper starts from a copy of the canonical message and writes mapped
values onto it, so untouched fields survive (the pipeline validates the full
result back into a CanonicalMessage).

There is intentionally NO "raw.*" business namespace: raw wire content stays
available only for audit/replay/debugging, never as the transformation
interface.
"""
import copy

from core.canonical_paths import assign_path, resolve_path
from core.message import Envelope
from core.model import CanonicalMessage
from nodes.base import TransformNode
from nodes.transform.functions import REGISTRY


class FieldMapper(TransformNode):
    def __init__(self, mappings: list[dict]):
        # mapping format: [{"source": "patient.name" | "lookups.ref.field",
        #                    "target": "patient.name",
        #                    "required": True,
        #                    "fn": "Uppercase" | None,
        #                    "fn_args": {} }]
        self.mappings = mappings

    def transform(self, canonical_dict: dict, lookups: dict | None = None) -> dict:
        """Maps canonical paths onto a copy of the canonical message's JSON
        form and returns the full updated dict."""
        out = copy.deepcopy(canonical_dict) if isinstance(canonical_dict, dict) else {}
        for m in self.mappings:
            src_key = m.get("source")
            target_key = m.get("target")
            if not src_key or not target_key:
                raise ValueError(f"mapping requires both source and target: {m!r}")

            if src_key.startswith("lookups."):
                val = resolve_path(lookups or {}, src_key[len("lookups."):])
            else:
                val = resolve_path(out, src_key)

            if val is None and m.get("required", False):
                raise ValueError(
                    f"Required field missing from canonical message: '{src_key}'"
                )

            fn_name = m.get("fn")
            if fn_name:
                fn = REGISTRY.get(fn_name)
                if fn is None:
                    raise ValueError(f"Unknown transform function: '{fn_name}'")
                val = fn(val, **(m.get("fn_args") or {}))

            assign_path(out, target_key, val)
        return out

    def apply(self, env: Envelope) -> Envelope:
        """Envelope wrapper: runs the mapping over the canonical message's
        JSON form (plus any enrichment results on env.lookups) and stores the
        validated result back as the envelope's canonical message."""
        out = self.transform(env.canonical_dict, env.lookups)
        env.canonical = CanonicalMessage.model_validate(out)
        return env
