# nodes/transform/field_mapper.py
from core.message import Envelope
from nodes.base import TransformNode
from nodes.transform.functions import REGISTRY


class FieldMapper(TransformNode):
    def __init__(self, mappings: list[dict]):
        # mapping format: [{"source": "order_id" | "lookups.patient.first_name",
        #                    "target": "accession_number",
        #                    "required": True,
        #                    "fn": "Uppercase" | None,
        #                    "fn_args": {} }]
        self.mappings = mappings

    def transform(self, raw_payload: dict, lookups: dict | None = None) -> dict:
        """Transforms a raw payload dict (plus any enrichment lookups) into
        mapped key-value pairs. Kept as a standalone method so it stays easy
        to unit test without an Envelope."""
        out = {}
        for m in self.mappings:
            src_key = m["source"]
            target_key = m["target"]
            is_required = m.get("required", False)

            val = self._resolve(src_key, raw_payload, lookups or {})
            if val is None and is_required:
                raise ValueError(f"Required field missing from payload: '{src_key}'")

            fn_name = m.get("fn")
            if fn_name:
                fn = REGISTRY.get(fn_name)
                if fn is None:
                    raise ValueError(f"Unknown transform function: '{fn_name}'")
                val = fn(val, **(m.get("fn_args") or {}))

            out[target_key] = val
        return out

    def _resolve(self, dotted_path: str, raw_payload: dict, lookups: dict):
        """Supports plain keys ('order_id'), explicit 'raw.<field>', and
        'lookups.<lookup_name>.<field>' for enrichment-attached data."""
        parts = dotted_path.split(".")
        if parts[0] == "lookups":
            cur = lookups
            parts = parts[1:]
        elif parts[0] == "raw":
            cur = raw_payload
            parts = parts[1:]
        else:
            cur = raw_payload

        for p in parts:
            if cur is None:
                return None
            cur = cur.get(p) if isinstance(cur, dict) else getattr(cur, p, None)
        return cur

    def apply(self, env: Envelope) -> Envelope:
        """Envelope wrapper: pulls from raw_payload plus any enrichment
        results already attached to env.lookups."""
        out = self.transform(env.raw_payload, env.lookups)
        env.transformed_payload = out
        return env
