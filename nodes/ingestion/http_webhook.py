import hashlib
import hmac

from flask import Blueprint, request, jsonify, abort

from core.message import Envelope


class WebhookRegistry:
    """One Flask blueprint, one route per channel: POST /webhooks/<channel_id>.
    Registered onto the same Flask app as the admin API/dashboard (api/app.py)
    so there's a single process and port to run, not one server per channel.

    Signature verification is optional per channel but strongly recommended —
    without a shared secret, anyone who finds the URL can inject fabricated
    messages into that channel's queue.
    """

    def __init__(self, queue):
        self.queue = queue
        self.bp = Blueprint("webhooks", __name__)
        self._channels: dict[str, dict] = {}
        self.bp.add_url_rule(
            "/webhooks/<channel_id>", view_func=self._handle, methods=["POST"]
        )

    def register(self, channel_id: str, shared_secret: str | None = None,
                 sig_header: str = "X-Signature", max_queue_depth: int | None = None,
                 idempotency_key_field: str | None = None):
        self._channels[channel_id] = {
            "secret": shared_secret,
            "sig_header": sig_header,
            "max_queue_depth": max_queue_depth,
            "idempotency_key_field": idempotency_key_field,
        }

    def unregister(self, channel_id: str):
        self._channels.pop(channel_id, None)

    def is_registered(self, channel_id: str) -> bool:
        return channel_id in self._channels

    def _handle(self, channel_id: str):
        cfg = self._channels.get(channel_id)
        if cfg is None:
            abort(404, description="unknown or disabled webhook channel")

        raw_body = request.get_data()

        if cfg["secret"]:
            sig = request.headers.get(cfg["sig_header"], "")
            expected = hmac.new(cfg["secret"].encode(), raw_body, hashlib.sha256).hexdigest()
            if not sig or not hmac.compare_digest(sig, expected):
                abort(401, description="invalid signature")

        payload = request.get_json(silent=True)
        if payload is None:
            abort(400, description="expected JSON body")

        max_depth = cfg.get("max_queue_depth")
        if max_depth is not None and self.queue.queue_depth(channel_id) >= max_depth:
            # backpressure: reject with 503 so a well-behaved sender retries
            # later (with backoff) instead of us silently dropping or
            # unboundedly growing the queue
            return jsonify({"status": "rejected", "reason": "queue at capacity"}), 503

        env = Envelope(channel_id=channel_id, raw_payload=payload)
        key_field = cfg.get("idempotency_key_field")
        if key_field and isinstance(payload, dict):
            env.idempotency_key = str(payload.get(key_field, "")) or None

        accepted = self.queue.enqueue(env)
        if not accepted:
            return jsonify({"status": "duplicate_ignored", "trace_id": env.trace_id}), 200
        return jsonify({"status": "accepted", "trace_id": env.trace_id}), 202
