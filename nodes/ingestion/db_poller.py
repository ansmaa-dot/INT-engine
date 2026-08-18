import json
import threading
import time

from core.transport import TransportMessage, to_envelope
from nodes.base import IngestionNode


class DBPoller(IngestionNode):
    """Polls a database on an interval and enqueues each row as a message.
    Supports SQLite, PostgreSQL, and MySQL via SQLAlchemy.

    Runs its own daemon thread via start()/stop() so it can sit alongside
    the existing per-channel worker thread in engine/main.py.
    """

    def __init__(self, connection_string: str, query: str, channel_id: str,
                 queue, db_type: str = "sqlite",
                 interval_s: float = 10,
                 cursor_field: str | None = None,
                 cursor_param: str | None = None,
                 max_queue_depth: int | None = None,
                 idempotency_key_field: str | None = None,
                 inbound_codec: str = "json"):
        if interval_s < 5:
            raise ValueError("interval_s must be >= 5")
        self.connection_string = connection_string
        self.query = query
        self.channel_id = channel_id
        self.queue = queue
        self.db_type = db_type.lower()
        self.interval_s = interval_s
        self.cursor_field = cursor_field
        self.cursor_param = cursor_param
        self.max_queue_depth = max_queue_depth
        self.idempotency_key_field = idempotency_key_field
        self.inbound_codec = inbound_codec
        self._cursor = None
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._engine = None

    def _get_engine(self):
        """Lazy-import SQLAlchemy and create the engine. We import here
        rather than at module top-level so the node definition is always
        importable even if SQLAlchemy isn't installed (e.g. on systems
        that only use other ingestion types)."""
        if self._engine is not None:
            return self._engine
        from sqlalchemy import create_engine
        self._engine = create_engine(self.connection_string)
        return self._engine

    def start(self, on_message_callback=None) -> None:
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True,
                                         name=f"db-poller-{self.channel_id}")
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()

    def _run(self):
        while not self._stop_event.is_set():
            try:
                self._poll_once()
            except Exception as e:
                print(f"[DBPoller:{self.channel_id}] error: {e}", flush=True)
            self._stop_event.wait(self.interval_s)

    def _poll_once(self):
        if self.max_queue_depth is not None:
            depth = self.queue.queue_depth(self.channel_id)
            if depth >= self.max_queue_depth:
                return

        engine = self._get_engine()
        params = {}
        if self.cursor_param:
            # Always pass the cursor param when configured — on the first
            # poll _cursor is None, so pass 0 to fetch everything (assuming
            # an integer cursor like `WHERE id > :cursor`).
            params[self.cursor_param] = self._cursor if self._cursor is not None else 0

        with engine.connect() as conn:
            from sqlalchemy import text
            result = conn.execute(text(self.query), params)

            column_names = list(result.keys())
            rows = [dict(zip(column_names, row)) for row in result.fetchall()]

        for row in rows:
            # Each row is delivered as raw serialized content in the new
            # input contract. The optional idempotency hint reads a column
            # from the already-structured row — not a wire-format parser.
            message_id = None
            if self.idempotency_key_field and isinstance(row, dict):
                message_id = str(row.get(self.idempotency_key_field, "")) or None
            msg = TransportMessage(
                raw=json.dumps(row) if isinstance(row, dict) else str(row),
                source=self.channel_id,
                message_id=message_id,
            )
            self.queue.enqueue(to_envelope(self.channel_id, msg, self.inbound_codec))
            if self.cursor_field and isinstance(row, dict):
                self._cursor = row.get(self.cursor_field, self._cursor)
