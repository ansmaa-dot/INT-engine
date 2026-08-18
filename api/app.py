"""Integration Engine — Flask control-plane application.

Orchestrates shared singletons and registers domain blueprints.
Run with::

    python -m api.app
"""

import sys
import os

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

def create_app() -> Flask:
    """Build and configure the Flask application."""
    app = Flask(__name__, template_folder="ui/templates")

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
    from api.ui.messages import bp as messages_bp        # /channels/<id>/messages/*
    from api.ui.dlq import bp as dlq_bp                  # /ui/dlq/*, /ui/audit, /api/audit/*
    from api.api_routes import bp as api_bp              # /api/*, /ui/test/simulate

    app.register_blueprint(channels_bp)
    app.register_blueprint(defs_bp)
    app.register_blueprint(messages_bp)
    app.register_blueprint(dlq_bp)
    app.register_blueprint(api_bp)

    # --- root / health (app-level, not in blueprints) ---
    @app.route("/")
    def index():
        return render_template("index.html")

    @app.route("/health")
    def health():
        configs = _registry.load_all_configs()
        running = sum(1 for c in configs.values()
                      if c.get("enabled", True) and c.get("status") == "running")
        return jsonify({"status": "ok", "channels_running": running})

    return app


# ---------------------------------------------------------------------------
# Module-level app instance (compatible with ``flask run`` / gunicorn)
# ---------------------------------------------------------------------------

app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

