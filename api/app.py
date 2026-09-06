"""Integration Engine — Flask control-plane application.

Orchestrates shared singletons and registers domain blueprints.
Run with::

    python -m api.app
"""

import sys
import os

from flask_wtf import CSRFProtect

# Ensure core / engine packages are importable regardless of execution path.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Flask, render_template, jsonify

from core.queue import PersistentQueue
from engine.config_loader import ChannelConfigRegistry, get_auth_manager
from nodes.ingestion.http_webhook import WebhookRegistry

from api.deps import queue, registry, auth_manager, webhooks, sync_webhooks


# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------

_HERE = os.path.dirname(os.path.abspath(__file__))


def create_app() -> Flask:
    """Build and configure the Flask application."""
    app = Flask(
        __name__,
        template_folder=os.path.join(_HERE, "ui", "templates"),
        static_folder=os.path.join(_HERE, "static"),
    )
    #csrf
    app.config["SECRET_KEY"] = os.environ.get("FLASK_SECRET_KEY", "dev-secret-key")
    CSRFProtect(app)
    # --- shared singletons ---
    _queue = PersistentQueue("queue.db")
    _registry = ChannelConfigRegistry("queue.db")
    _auth_manager = get_auth_manager()
    _webhooks = WebhookRegistry(_queue)

    # Push into the module-level deps so every blueprint can import them.
    import api.deps as deps
    deps.queue = _queue
    deps.registry = _registry
    deps.auth_manager = _auth_manager
    deps.webhooks = _webhooks

    app.register_blueprint(_webhooks.bp)
    sync_webhooks()

    # --- domain blueprints ---
    from api.ui.channels import bp as channels_bp        # /ui/metrics, /ui/channels/*
    from api.ui.definitions import bp as defs_bp         # /ui/defs/*
    from api.ui.fields import bp as fields_bp             # /ui/fields/*, /ui/preview, /ui/enrichments/columns
    from api.ui.messages import bp as messages_bp        # /channels/<id>/messages/*
    from api.ui.dlq import bp as dlq_bp                  # /ui/dlq/*, /ui/audit, /api/audit/*
    from api.api_routes import bp as api_bp              # /api/*, /ui/test/simulate

    app.register_blueprint(channels_bp)
    app.register_blueprint(defs_bp)
    app.register_blueprint(fields_bp)
    app.register_blueprint(messages_bp)
    app.register_blueprint(dlq_bp)
    app.register_blueprint(api_bp)

    # --- root / health (app-level, not in blueprints) ---
    @app.route("/")
    def index():
        return render_template("index.html")

    @app.route("/health")
    def health():
        from flask import request
        configs = _registry.load_all_configs()
        running = sum(1 for c in configs.values()
                      if c.get("enabled", True) and c.get("status") == "running")
        age = _queue.get_heartbeat_age_s()
        if age is None:
            engine_status = "OFFLINE"
        elif age < 10:
            engine_status = "ONLINE"
        else:
            engine_status = f"STALE ({int(age)}s)"
        if request.headers.get("HX-Request"):
            color = "#16a34a" if engine_status == "ONLINE" else "#dc2626"
            return f'Engine: <strong style="color:{color}">{engine_status}</strong> &nbsp;|&nbsp; Channels: <strong>{running}</strong>'
        return jsonify({"status": "ok", "channels_running": running, "engine": engine_status})

    return app


# ---------------------------------------------------------------------------
# Module-level app instance (compatible with ``flask run`` / gunicorn)
# ---------------------------------------------------------------------------

app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

