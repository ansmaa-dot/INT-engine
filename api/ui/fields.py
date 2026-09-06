"""Fields blueprint — field catalog, sample preview, and enrichment column
discovery endpoints for the step-chain channel builder UI.

All three endpoints are HTMX-friendly JSON endpoints consumed by the
front-end step-chain builder (P5).
"""

import json

from flask import Blueprint, jsonify, request

from api.ui.helpers import _esc
from core.field_catalog import (
    FieldDescriptor,
    catalog_for_codec,
    human_label,
)
from nodes.codec import get as get_codec
from nodes.codec.registry import CodecNotFoundError
from nodes.enrichment.db_adapter import (
    build_adapter,
    is_valid_identifier,
)

bp = Blueprint("ui_fields", __name__)


# ---------------------------------------------------------------------------
# Helper: serialize a FieldDescriptor to a JSON-safe dict
# ---------------------------------------------------------------------------

def _field_to_dict(d: FieldDescriptor) -> dict:
    """Serialize a FieldDescriptor to a dict, including transient ``_hint``."""
    obj: dict = {
        "path": d.path,
        "label": d.label,
        "kind": d.kind,
        "group": d.group,
    }
    hint = getattr(d, "_hint", None)
    if hint:
        obj["hint"] = hint
    return obj
# ---------------------------------------------------------------------------
# GET  /ui/fields/<codec>
# ---------------------------------------------------------------------------

@bp.route("/ui/fields/<codec>")
def get_fields(codec):
    """Return the canonical field catalog for *codec*, with per-codec
    source-format hints when available.

    Codec key must match ``nodes.codec.registry`` exactly
    (``hl7v2.5.1.ORU_R01``, ``fhir.r4``, …).
    """
    # Validate that the codec key is known — an unknown key returns an
    # empty catalog with no hints (the UI can still show labels).
    try:
        get_codec(codec)
    except CodecNotFoundError:
        pass

    catalog = catalog_for_codec(codec)
    fields = [_field_to_dict(d) for d in catalog]
    return jsonify({"fields": fields})


# ---------------------------------------------------------------------------
# POST  /ui/preview
# ---------------------------------------------------------------------------

def _walk_canonical(obj, prefix: str, *, label_fn=None) -> list[dict]:
    """Walk a canonical-model object (or list / scalar) and return a flat
    list of {path, label, kind, sample_value} dicts."""
    from datetime import date, datetime

    result: list[dict] = []

    def _label(path: str) -> str:
        if label_fn:
            lbl = label_fn(path)
            if lbl:
                return lbl
        return path

    def _kind_from_value(val) -> str:
        if isinstance(val, (date, datetime)):
            return "datetime"
        if isinstance(val, list):
            return "list"
        return "str"

    def _walk(value, prefix_path: str, depth: int):
        if depth > 20:
            return
        if value is None:
            return

        if isinstance(value, (str, int, float, bool, date, datetime)):
            kind = _kind_from_value(value)
            sval = value.isoformat() if isinstance(value, (date, datetime)) else str(value)
            result.append({
                "path": prefix_path,
                "label": _label(prefix_path),
                "kind": kind,
                "sample_value": sval[:200],
            })
            return

        if isinstance(value, list):
            for idx, item in enumerate(value):
                item_prefix = f"{prefix_path}.{idx}" if prefix_path else str(idx)
                _walk(item, item_prefix, depth + 1)
            return

        if isinstance(value, dict):
            for k, v in value.items():
                child_prefix = f"{prefix_path}.{k}" if prefix_path else k
                _walk(v, child_prefix, depth + 1)
            return

        if hasattr(value, "model_dump"):
            d = value.model_dump(exclude_none=False)
            for key, val in d.items():
                child_prefix = f"{prefix_path}.{key}" if prefix_path else key
                _walk(val, child_prefix, depth + 1)
            return

        sval = str(value)[:200]
        result.append({
            "path": prefix_path,
            "label": _label(prefix_path),
            "kind": "str",
            "sample_value": sval,
        })

    _walk(obj, "", 0)
    result.sort(key=lambda r: r["path"])
    return result


@bp.route("/ui/preview", methods=["POST"])
def preview_sample():
    """Parse a sample payload through the given codec and return a flat
    field tree.

    Request body (JSON):
        {"codec": "hl7v2.5.1.ORU_R01", "sample": "MSH|…"}

    On success returns ``{"fields": [{path, label, kind, sample_value}]}``.
    On decode error returns a typed error dict (no traceback).
    """
    body = request.get_json(silent=True) or {}
    codec_key = (body.get("codec") or "").strip()
    sample = body.get("sample") or ""

    if not codec_key:
        return jsonify({"error": "codec is required"}), 400

    try:
        codec = get_codec(codec_key)
    except CodecNotFoundError:
        return jsonify({"error": f"unknown codec: {_esc(codec_key)}"}), 400

    from core.errors import DecodeError, CanonicalValidationError

    try:
        canonical = codec.parse(sample)
    except (DecodeError, CanonicalValidationError) as e:
        return jsonify({
            "error": {
                "stage": getattr(e, "stage", "decode"),
                "code": getattr(e, "code", "error"),
                "message": str(e),
            }
        }), 422
    except Exception as e:
        return jsonify({
            "error": {
                "stage": "decode",
                "code": "decode.failed",
                "message": f"Failed to parse sample: {str(e)[:500]}",
            }
        }), 422

    fields = _walk_canonical(canonical, "", label_fn=human_label)
# ---------------------------------------------------------------------------
# POST  /ui/enrichments/columns
# ---------------------------------------------------------------------------

@bp.route("/ui/enrichments/columns", methods=["POST"])
def enrich_columns():
    """Return column names for a database table, guarded by identifier
    validation.

    Request body (JSON):
        {"db_type": "sqlite", "db_path": "...", "table": "patients"}

    Returns ``{"columns": [...]}``.
    """
    body = request.get_json(silent=True) or {}
    db_type = (body.get("db_type") or "sqlite").strip().lower()
    table = (body.get("table") or "").strip()

    if not table:
        return jsonify({"error": "table name is required"}), 400

    if not is_valid_identifier(table):
        return jsonify({"error": f"invalid table name: {_esc(table)}"}), 400

    try:
        if db_type == "sqlite":
            db_path = body.get("db_path") or ""
            if not db_path:
                return jsonify({"error": "db_path is required for sqlite"}), 400
            adapter = build_adapter("sqlite", db_path=db_path)
        elif db_type in ("postgresql", "postgres"):
            conn_str = body.get("connection_string") or ""
            if not conn_str:
                return jsonify({"error": "connection_string is required for postgresql"}), 400
            adapter = build_adapter("postgresql", connection_string=conn_str)
        elif db_type == "mysql":
            host = body.get("host") or ""
            port = int(body.get("port") or 0)
            user = body.get("user") or ""
            password = body.get("password") or ""
            database = body.get("database") or ""
            if not all([host, port, user, database]):
                return jsonify({"error": "host, port, user, database required for mysql"}), 400
            adapter = build_adapter(
                "mysql", host=host, port=port, user=user,
                password=password, database=database,
            )
        else:
            return jsonify({"error": f"unsupported db_type: {_esc(db_type)}"}), 400
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    try:
        if not adapter.table_exists(table):
            return jsonify({"error": f"table {_esc(table)!r} not found"}), 404
        columns = adapter.column_names(table)
    except Exception as e:
        return jsonify({"error": f"failed to read columns: {str(e)[:500]}"}), 500

    return jsonify({"columns": columns})
# ---------------------------------------------------------------------------
# Step-type metadata (exported for use by channels.py during render)
# ---------------------------------------------------------------------------

STEP_TYPE_META = {
    "enrich": {
        "label": "Enrich",
        "badge_class": "step-badge-enrich",
        "description": "Database lookup — add reference data",
        "on_fail_options": [],
    },
    "transform": {
        "label": "Transform",
        "badge_class": "step-badge-transform",
        "description": "Field mapping — reshape/rename fields",
        "on_fail_options": [],
    },
    "filter": {
        "label": "Filter",
        "badge_class": "step-badge-filter",
        "description": "Boolean expression — drop or dead-letter non-matching",
        "on_fail_options": [
            {"value": "dead_letter", "label": "Route to dead-letter queue"},
            {"value": "discard", "label": "Discard silently"},
        ],
    },
    "assert": {
        "label": "Assert",
        "badge_class": "step-badge-assert",
        "description": "Validation expression — retry or dead-letter on failure",
        "on_fail_options": [
            {"value": "retry", "label": "Retry (transient failure)"},
            {"value": "dead_letter", "label": "Send to dead-letter queue"},
        ],
    },
}