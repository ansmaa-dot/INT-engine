"""Shared singletons for the API layer.

Module-level references initialized by app.py before any request is handled.
All blueprint modules import from here to access queue, registry, etc.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.queue import PersistentQueue
    from engine.config_loader import ChannelConfigRegistry
    from core.auth_manager import AuthManager
    from nodes.ingestion.http_webhook import WebhookRegistry

#: Compiled-once validator for channel-id characters.
CHANNEL_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")

# These are set by app.create_app() / app.init_deps() before any route handles a request.
queue: "PersistentQueue" = None         # type: ignore[assignment]
registry: "ChannelConfigRegistry" = None  # type: ignore[assignment]
auth_manager: "AuthManager" = None       # type: ignore[assignment]
webhooks: "WebhookRegistry" = None       # type: ignore[assignment]


def sync_webhooks() -> None:
    """Register / unregister webhook routes so the live Flask process matches
    the database without a restart.  Called at startup and after any
    create / edit / delete / toggle of channels."""
    configs = registry.load_all_configs()
    live_ids: set[str] = set()
    for cid, conf in configs.items():
        if conf.get("enabled", True) and conf.get("inbound_transport") == "http_webhook":
            icfg = conf.get("inbound_transport_config") or {}
            webhooks.register(
                cid,
                shared_secret=icfg.get("shared_secret") or None,
                sig_header=icfg.get("sig_header", "X-Signature"),
                max_queue_depth=icfg.get("max_queue_depth"),
                idempotency_key_field=icfg.get("idempotency_key_field"),
                inbound_codec=conf.get("inbound_codec", "json"),
            )
            live_ids.add(cid)
    for cid in list(webhooks._channels.keys()):
        if cid not in live_ids:
            webhooks.unregister(cid)
