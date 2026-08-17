import threading
import time

import requests

from core.message import Envelope
from core.auth_manager import AuthManager
from nodes.base import IngestionNode


class HTTPPoller(IngestionNode):
    """Polls a REST endpoint on an interval and enqueues each record it
    finds. Runs its own daemon thread via start()/stop() so it can sit
    alongside the existing per-channel worker thread in engine/main.py."""

    def __init__(self, url: str, channel_id: str, queue, auth: AuthManager,
                 auth_profile_id: str | None = None, interval_s: float = 10,
                 records_path: str = "", cursor_param: str | None = None,
                 cursor_field: str | None = None, max_queue_depth: int | None = None,
                 idempotency_key_field: str | None = None):
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
        self._cursor = None
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self, on_message_callback=None) -> None:
        """on_message_callback is unused here (kept for IngestionNode
        interface parity) — HTTPPoller enqueues directly, same as the
        webhook receiver, since both write straight to the persistent queue."""
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
                # source API — the cursor stays put so nothing is skipped
                return

        headers = self.auth.get_headers(self.auth_profile_id) if self.auth else {}
        params = {}
        if self.cursor_param and self._cursor is not None:
            params[self.cursor_param] = self._cursor

        resp = requests.get(self.url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        body = resp.json()

        records = self._extract(body)
        for record in records:
            env = Envelope(channel_id=self.channel_id, raw_payload=record)
            if self.idempotency_key_field and isinstance(record, dict):
                env.idempotency_key = str(record.get(self.idempotency_key_field, "")) or None
            self.queue.enqueue(env)
            if self.cursor_field and isinstance(record, dict):
                self._cursor = record.get(self.cursor_field, self._cursor)

    def _extract(self, body) -> list:
        if not self.records_path:
            return body if isinstance(body, list) else [body]
        cur = body
        for p in self.records_path.split("."):
            cur = cur.get(p, []) if isinstance(cur, dict) else cur
        return cur or []
