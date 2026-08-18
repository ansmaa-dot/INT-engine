from datetime import datetime, timedelta, timezone
import json
import sqlite3
import uuid
from core.message import Envelope, MessageState
from core.model import CanonicalMessage


def _json_text(obj) -> str | None:
    """Serialize an object for a TEXT column.

    CanonicalMessage instances are dumped via pydantic's JSON mode so the
    stored JSON exactly matches what ``Envelope.from_dict`` parses back.
    """
    if obj is None:
        return None
    if isinstance(obj, CanonicalMessage):
        return json.dumps(obj.model_dump(mode="json"))
    if isinstance(obj, (dict, list)):
        return json.dumps(obj, default=str)
    return str(obj)


class PersistentQueue:

    def __init__(self, db_path="queue.db"):
        self.db_path = db_path
        self._init_db()

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        # busy_timeout lets concurrent workers/processes wait out a writer
        # instead of failing immediately with "database is locked"
        conn.execute("PRAGMA busy_timeout=30000;")
        return conn

    def _init_db(self):
        with self._get_conn() as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS queue (
                    trace_id TEXT PRIMARY KEY,
                    channel_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    attempts INTEGER DEFAULT 0,
                    raw TEXT,
                    inbound_codec TEXT NOT NULL DEFAULT 'json',
                    canonical TEXT,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    next_retry_at TEXT,
                    idempotency_key TEXT,
                    claimed_at TEXT,
                    claim_token TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS idempotency_keys (
                    channel_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    trace_id TEXT NOT NULL,
                    seen_at TEXT NOT NULL,
                    PRIMARY KEY (channel_id, idempotency_key)
                )
            """)
            # full lifecycle history — unlike the `queue` table (current state
            # only), this is append-only so "what did we receive/send for
            # trace X at 14:03" stays answerable after the message leaves
            # the active queue or gets deleted from DLQ.
            conn.execute("""
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trace_id TEXT NOT NULL,
                    channel_id TEXT NOT NULL,
                    event TEXT NOT NULL,
                    detail TEXT,
                    at TEXT NOT NULL
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_trace ON audit_log (trace_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_queue_channel_state ON queue (channel_id, state)")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS heartbeat (
                    id INTEGER PRIMARY KEY,
                    last_beat_at TEXT NOT NULL
                )
            """)
            conn.commit()

    # --- idempotency -----------------------------------------------------

    def claim_idempotency_key(self, channel_id: str, idempotency_key: str, trace_id: str) -> bool:
        """Returns True if this is the first time this key has been seen for
        this channel (and records it), False if it's a duplicate. The insert
        itself is the atomic check — no separate SELECT-then-INSERT race."""
        now_iso = datetime.now(timezone.utc).isoformat()
        try:
            with self._get_conn() as conn:
                conn.execute(
                    "INSERT INTO idempotency_keys (channel_id, idempotency_key, trace_id, seen_at) VALUES (?, ?, ?, ?)",
                    (channel_id, idempotency_key, trace_id, now_iso),
                )
                conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    # --- enqueue / dequeue ------------------------------------------------

    def enqueue(self, envelope: Envelope) -> bool:
        """Returns False (and skips insertion) if the envelope carries an
        idempotency_key that's already been seen for this channel. Returns
        True otherwise, including for envelopes with no idempotency_key set
        (idempotency is opt-in per channel)."""
        if envelope.idempotency_key:
            accepted = self.claim_idempotency_key(
                envelope.channel_id, envelope.idempotency_key, envelope.trace_id
            )
            if not accepted:
                self.record_audit(envelope.trace_id, envelope.channel_id, "duplicate_rejected",
                                   {"idempotency_key": envelope.idempotency_key})
                return False

        state_str = (
            envelope.state.value
            if hasattr(envelope.state, "value")
            else str(envelope.state)
        ).upper()
        now_iso = datetime.now(timezone.utc).isoformat()

        with self._get_conn() as conn:
            conn.execute(
                """
                INSERT INTO queue (trace_id, channel_id, state, attempts, raw,
                                   inbound_codec, canonical, error, created_at,
                                   next_retry_at, idempotency_key)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    envelope.trace_id,
                    envelope.channel_id,
                    state_str,
                    envelope.attempts,
                    envelope.raw,
                    envelope.inbound_codec,
                    _json_text(envelope.canonical),
                    _json_text(envelope.error),
                    envelope.created_at,
                    now_iso,
                    envelope.idempotency_key,
                ),
            )
            conn.commit()

        self.record_audit(envelope.trace_id, envelope.channel_id, "queued",
                           {"raw": envelope.raw, "inbound_codec": envelope.inbound_codec})
        return True

    def dequeue_available(self, channel_id: str):
        """Atomically claims the next available QUEUED message due for
        processing and flips it to PROCESSING in the same statement, so two
        worker threads/processes racing on the same channel never both pull
        the same row (the UPDATE...WHERE only matches — and thus only
        affects — one row, and SQLite serializes writers)."""
        now_iso = datetime.now(timezone.utc).isoformat()
        queued = MessageState.QUEUED.value if hasattr(MessageState.QUEUED, "value") else str(MessageState.QUEUED)
        processing = MessageState.PROCESSING.value if hasattr(MessageState.PROCESSING, "value") else str(MessageState.PROCESSING)
        queued, processing = queued.upper(), processing.upper()

        claim_token = uuid.uuid4().hex

        with self._get_conn() as conn:
            cur = conn.execute(
                """
                UPDATE queue
                SET state = ?, claimed_at = ?, claim_token = ?
                WHERE trace_id = (
                    SELECT trace_id FROM queue
                    WHERE channel_id = ?
                      AND UPPER(state) = ?
                      AND (next_retry_at IS NULL OR next_retry_at <= ?)
                    ORDER BY created_at ASC
                    LIMIT 1
                )
                AND UPPER(state) = ?
                """,
                (processing, now_iso, claim_token, channel_id, queued, now_iso, queued),
            )
            if cur.rowcount == 0:
                conn.commit()
                return None

            # look up by claim_token, not by "earliest PROCESSING row" — under
            # concurrency, several rows can be PROCESSING at once (other
            # threads' claims); the token guarantees we read back exactly
            # the row THIS call just claimed, not someone else's.
            row = conn.execute(
                "SELECT * FROM queue WHERE claim_token = ?", (claim_token,)
            ).fetchone()
            conn.commit()

        return Envelope.from_dict(dict(row)) if row else None

    # --- state transitions -------------------------------------------------

    def mark_retry(self, trace_id: str, attempts: int, delay_seconds: int, error: dict):
        """Schedules a message for retry using exponential backoff. ``error``
        is the structured pipeline error dict (stage/code/message/traceback)."""
        next_retry = (
            datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)
        ).isoformat()
        queued_state = (
            MessageState.QUEUED.value
            if hasattr(MessageState.QUEUED, "value")
            else str(MessageState.QUEUED)
        ).upper()

        with self._get_conn() as conn:
            conn.execute(
                """
                UPDATE queue 
                SET state = ?, attempts = ?, error = ?, next_retry_at = ?
                WHERE trace_id = ?
            """,
                (queued_state, attempts, _json_text(error), next_retry, trace_id),
            )
            conn.commit()

        self.record_audit(trace_id, self._channel_for(trace_id), "retry_scheduled",
                           {"attempts": attempts, "delay_seconds": delay_seconds, "error": error})

    def mark_dead_letter(self, trace_id: str, attempts: int, error: dict):
        """Permanently moves message to DLQ after exhausting retry attempts.
        ``error`` is the structured pipeline error dict."""
        dlq_state = (
            MessageState.DEAD_LETTER.value
            if hasattr(MessageState.DEAD_LETTER, "value")
            else str(MessageState.DEAD_LETTER)
        ).upper()

        with self._get_conn() as conn:
            conn.execute(
                """
                UPDATE queue 
                SET state = ?, attempts = ?, error = ?, next_retry_at = NULL
                WHERE trace_id = ?
            """,
                (dlq_state, attempts, _json_text(error), trace_id),
            )
            conn.commit()

        self.record_audit(trace_id, self._channel_for(trace_id), "dead_lettered",
                           {"attempts": attempts, "error": error})

    def mark_delivered(self, trace_id: str, canonical: CanonicalMessage | None, attempts: int = 0):
        delivered_state = (
            MessageState.DELIVERED.value
            if hasattr(MessageState.DELIVERED, "value")
            else str(MessageState.DELIVERED)
        ).upper()

        with self._get_conn() as conn:
            conn.execute(
                """
                UPDATE queue 
                SET state = ?, attempts = ?, canonical = ?, next_retry_at = NULL
                WHERE trace_id = ?
            """,
                (delivered_state, attempts, _json_text(canonical), trace_id),
            )
            conn.commit()

        self.record_audit(trace_id, self._channel_for(trace_id), "delivered",
                           {"canonical": canonical.model_dump(mode="json") if canonical else None})

    def _channel_for(self, trace_id: str) -> str:
        with self._get_conn() as conn:
            row = conn.execute("SELECT channel_id FROM queue WHERE trace_id = ?", (trace_id,)).fetchone()
        return row["channel_id"] if row else ""

    # --- audit trail -------------------------------------------------------

    def record_audit(self, trace_id: str, channel_id: str, event: str, detail: dict | None = None):
        now_iso = datetime.now(timezone.utc).isoformat()
        with self._get_conn() as conn:
            conn.execute(
                "INSERT INTO audit_log (trace_id, channel_id, event, detail, at) VALUES (?, ?, ?, ?, ?)",
                (trace_id, channel_id, event, json.dumps(detail) if detail is not None else None, now_iso),
            )
            conn.commit()

    def get_audit_trail(self, trace_id: str) -> list[dict]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT event, detail, at FROM audit_log WHERE trace_id = ? ORDER BY id ASC",
                (trace_id,),
            ).fetchall()
        out = []
        for r in rows:
            detail = None
            if r["detail"]:
                try:
                    detail = json.loads(r["detail"])
                except Exception:
                    detail = r["detail"]
            out.append({"event": r["event"], "detail": detail, "at": r["at"]})
        return out

    # --- backpressure --------------------------------------------------------

    def queue_depth(self, channel_id: str) -> int:
        """Count of QUEUED (not yet PROCESSING/DELIVERED/DEAD_LETTER) messages
        for a channel. Ingestion nodes check this against a configured
        max_queue_depth before accepting more work."""
        with self._get_conn() as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM queue WHERE channel_id = ? AND UPPER(state) = 'QUEUED'",
                (channel_id,),
            ).fetchone()[0]

    def update_heartbeat(self) -> None:
        """Called by the worker daemon to signal it's alive."""
        now_iso = datetime.now(timezone.utc).isoformat()
        with self._get_conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO heartbeat (id, last_beat_at) VALUES (1, ?)",
                (now_iso,),
            )
            conn.commit()

    def get_heartbeat_age_s(self) -> float | None:
        """Returns seconds since the last heartbeat, or None if never set."""
        with self._get_conn() as conn:
            row = conn.execute("SELECT last_beat_at FROM heartbeat WHERE id = 1").fetchone()
        if not row or not row["last_beat_at"]:
            return None
        last = datetime.fromisoformat(row["last_beat_at"])
        return (datetime.now(timezone.utc) - last).total_seconds()

    def reclaim_stale_processing(self, older_than_seconds: int = 120) -> int:
        """If a worker crashes between claiming a message (PROCESSING) and
        marking it delivered/retried/dead-lettered, the message would
        otherwise sit stuck forever — no worker will ever pick up a
        PROCESSING row again. Called periodically by the worker daemon to
        put anything claimed too long ago back into QUEUED. Returns the
        number of rows reclaimed."""
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=older_than_seconds)).isoformat()
        with self._get_conn() as conn:
            cur = conn.execute(
                """
                UPDATE queue
                SET state = 'QUEUED', claimed_at = NULL
                WHERE UPPER(state) = 'PROCESSING' AND claimed_at IS NOT NULL AND claimed_at <= ?
                """,
                (cutoff,),
            )
            conn.commit()
            return cur.rowcount
