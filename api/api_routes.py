"""API & test endpoints blueprint — /api/* endpoints and test simulation."""

import json

from flask import Blueprint, jsonify, request

from api.deps import registry, queue
from api.ui.helpers import _esc
from core.message import Envelope

bp = Blueprint("api_routes", __name__)


@bp.route("/api/config/validate", methods=["POST"])
def validate_config():
    data = request.get_json(silent=True) or {}
    errors = registry.validate_channel_definition(data)
    hints = registry.channel_shape_hints(data)
    return jsonify({"valid": len(errors) == 0, "errors": errors, "hints": hints})


@bp.route("/api/ingest/<channel_id>", methods=["POST"])
def ingest_message(channel_id):
    data = request.get_json(silent=True) or request.form.to_dict()
    if not data:
        return jsonify({"error": "Empty or non-JSON payload"}), 400

    conf = registry.load_config(channel_id) or {}
    icfg = conf.get("inbound_transport_config") or {}

    max_depth = icfg.get("max_queue_depth")
    if max_depth is not None and queue.queue_depth(channel_id) >= max_depth:
        return jsonify({"status": "rejected", "reason": "queue at capacity"}), 503

    env = Envelope(channel_id=channel_id, raw=json.dumps(data),
                   inbound_codec=conf.get("inbound_codec", "json"))
    key_field = icfg.get("idempotency_key_field")
    if key_field and isinstance(data, dict):
        env.idempotency_key = str(data.get(key_field, "")) or None

    accepted = queue.enqueue(env)
    if not accepted:
        return jsonify({"status": "duplicate_ignored", "trace_id": env.trace_id}), 200
    return jsonify({"status": "queued", "trace_id": env.trace_id}), 201


@bp.route("/ui/test/simulate", methods=["POST"])
def simulate_test_messages():
    # reuse get_ui_metrics from the channels blueprint
    from api.ui.channels import get_ui_metrics

    queue.enqueue(
        Envelope(
            channel_id="his_to_lis",
            raw=json.dumps({"order_id": 9001, "doctor_username": "dr_smith", "test": "COMPREHENSIVE_METABOLIC"}),
        )
    )

    queue.enqueue(
        Envelope(channel_id="his_to_lis", raw=json.dumps({"test": "LIPID_PANEL"}))
    )

    return get_ui_metrics()
