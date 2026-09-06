import json

from core.message import Envelope, MessageState
from core.model import CanonicalMessage, Identifier, PatientSummary
from core.queue import PersistentQueue


def _mkqueue(tmp_path) -> PersistentQueue:
    return PersistentQueue(str(tmp_path / "q.db"))


def test_enqueue_dequeue_persists_new_shape(tmp_path):
    q = _mkqueue(tmp_path)
    canonical = CanonicalMessage(patient=PatientSummary(name="Ada"))
    env = Envelope(channel_id="c1", raw='{"patient": {"name": "Ada"}}',
                   inbound_codec="json", canonical=canonical)
    assert q.enqueue(env) is True

    got = q.dequeue_available("c1")
    assert got is not None
    assert got.channel_id == "c1"
    assert got.raw == '{"patient": {"name": "Ada"}}'
    assert got.inbound_codec == "json"
    assert got.canonical == canonical
    assert got.state == MessageState.PROCESSING


def test_enqueue_stores_error(tmp_path):
    q = _mkqueue(tmp_path)
    err = {"stage": "decode", "code": "json.invalid", "message": "bad"}
    env = Envelope(channel_id="c1", raw="x", error=err)
    q.enqueue(env)
    got = q.dequeue_available("c1")
    assert got.error == err


def test_delivered_persists_canonical(tmp_path):
    q = _mkqueue(tmp_path)
    env = Envelope(channel_id="c1", raw="{}")
    q.enqueue(env)
    canonical = CanonicalMessage(patient=PatientSummary(identifiers=[Identifier(value="42")]))
    q.mark_delivered(env.trace_id, canonical)

    assert q.dequeue_available("c1") is None  # not queued anymore
    with q._get_conn() as conn:
        row = conn.execute("SELECT state, canonical FROM queue WHERE trace_id = ?",
                           (env.trace_id,)).fetchone()
    assert row["state"] == MessageState.DELIVERED.value
    assert CanonicalMessage.model_validate_json(row["canonical"]) == canonical


def test_retry_then_dead_letter_persists_error(tmp_path):
    q = _mkqueue(tmp_path)
    env = Envelope(channel_id="c1", raw="{}")
    q.enqueue(env)
    q.dequeue_available("c1")  # claim -> PROCESSING

    err = {"stage": "destination", "code": "destination.failed", "message": "boom"}
    q.mark_retry(env.trace_id, 1, 0, err)

    got = q.dequeue_available("c1")  # next_retry_at is now -> due
    assert got is not None
    assert got.state == MessageState.PROCESSING
    assert got.error == err

    q.mark_dead_letter(env.trace_id, 2, err)
    with q._get_conn() as conn:
        row = conn.execute("SELECT state, error FROM queue WHERE trace_id = ?",
                           (env.trace_id,)).fetchone()
    assert row["state"] == MessageState.DEAD_LETTER.value
    assert json.loads(row["error"]) == err


def test_reclaim_stale_processing(tmp_path):
    q = _mkqueue(tmp_path)
    env = Envelope(channel_id="c1", raw="{}")
    q.enqueue(env)
    q.dequeue_available("c1")  # PROCESSING, claimed_at = now
    assert q.reclaim_stale_processing(older_than_seconds=0) == 1
    assert q.dequeue_available("c1") is not None


def test_idempotency_duplicate_rejected(tmp_path):
    q = _mkqueue(tmp_path)
    a = Envelope(channel_id="c1", raw="1", idempotency_key="k")
    b = Envelope(channel_id="c1", raw="2", idempotency_key="k")
    assert q.enqueue(a) is True
    assert q.enqueue(b) is False


def test_mark_discarded_persists_state_and_audit(tmp_path):
    """Filter on_fail=discard writes DISCARDED state + 'discarded' audit event,
    not DEAD_LETTER + 'dead_lettered' — they must never merge (D6/D9)."""
    q = _mkqueue(tmp_path)
    env = Envelope(channel_id="c1", raw="{}")
    q.enqueue(env)
    q.dequeue_available("c1")  # claim -> PROCESSING

    err = {"stage": "filter", "code": "filter.expr", "message": "excluded by filter"}
    q.mark_discarded(env.trace_id, 1, err)

    # Not queued for reprocessing
    assert q.dequeue_available("c1") is None

    # DB row state
    with q._get_conn() as conn:
        row = conn.execute(
            "SELECT state, error FROM queue WHERE trace_id = ?",
            (env.trace_id,),
        ).fetchone()
    assert row["state"] == MessageState.DISCARDED.value
    assert json.loads(row["error"]) == err

    # Audit trail records 'discarded', not 'dead_lettered'
    trail = q.get_audit_trail(env.trace_id)
    events = [e["event"] for e in trail]
    assert "discarded" in events
    assert "dead_lettered" not in events
    q = _mkqueue(tmp_path)
    q.enqueue(Envelope(channel_id="c1", raw="1"))
    q.enqueue(Envelope(channel_id="c1", raw="2"))
    assert q.queue_depth("c1") == 2
    q.dequeue_available("c1")
    assert q.queue_depth("c1") == 1