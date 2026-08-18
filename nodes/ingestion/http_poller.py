import threading
import time

import requests

from core.auth_manager import AuthManager
from core.transport import TransportMessage, to_envelope
from nodes.base import IngestionNode


class HTTPPoller(IngestionNode):
    """Polls a REST endpoint on an interval and enqueues the raw response
    body (`TransportMessage`). It runs its own daemon thread via start()/stop()
    so it can sit alongside the existing per-channel worker thread in
    engine/main.py.

    The poller is deliberately format-agnostic: it returns the raw response
    body plus response metadata. Record extraction / parsing is the codec's
    job in the pipeline — not the transport's.

    `records_path` / `cursor_field` are retained for config compatibility but
    are no longer used (record-aware extraction moves to a codec in a later
    pass).
    """

    def __init__(self, url: str, channel_id: str, queue, auth: AuthManager,
                 auth_profile_id: str | None = None, interval_s: float = 10,
                 records_path: str = "", cursor_param: str | None = None,
                 cursor_field: str | None = None, max_queue_depth: int | None = None,
                 idempotency_key_field: str | None = None,
                 inbound_codec: str = "json"):
        if interval_s < 5:
            raise ValueError("interval_s must be >= 5")
        self.url = url
        self.channel_id = channel_id
        self.queue = queue
        self.auth = auth
        self.auth_profile_id = auth_profile_id
        self.interval_s = interval_s
        self.records_path = records_path
        self.cursor_param = cursor_param
        self.cursor_field = cursor_field
        self.max_queue_depth = max_queue_depth
        self.idempotency_key_field = idempotency_key_field
        self.inbound_codec = inbound_codec
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self, on_message_callback=None) -> None:
        """on_message_callback is unused here (kept for IngestionNode
        interface parity)."""
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True,
                                         name=f"http-poller-{self.channel_id}")
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()

    def _run(self):
        while not self._stop_event.is_set():
            try:
                self._poll_once()
            except requests.RequestException as e:
                print(f"[HTTPPoller:{self.channel_id}] transient error: {e}", flush=True)
            except Exception as e:
                print(f"[HTTPPoller:{self.channel_id}] error: {e}", flush=True)
            self._stop_event.wait(self.interval_s)

    def _poll_once(self):
        if self.max_queue_depth is not None:
            depth = self.queue.queue_depth(self.channel_id)
            if depth >= self.max_queue_depth:
                # backpressure: skip this tick entirely, don't even hit the
                # source API; nothing is dropped
                return

        headers = self.auth.get_headers(self.auth_profile_id) if self.auth else {}
        resp = requests.get(self.url, headers=headers, timeout=10)
        resp.raise_for_status()

        # Raw response body + response metadata only — no record extraction.
        msg = TransportMessage(
            raw=resp.content,
            source=self.channel_id,
            content_type=resp.headers.get("Content-Type"),
        )
        self.queue.enqueue(to_envelope(self.channel_id, msg, self.inbound_codec))
