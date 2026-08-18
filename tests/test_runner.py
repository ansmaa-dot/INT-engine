import json

from core.message import Envelope
from core.queue import PersistentQueue
from engine.runner import ChannelRunner
from nodes.transform.field_mapper import FieldMapper


class RecordingDestination:
    def __init__(self, fail=False):
        self.fail = fail
        self.sent = []

    def send(self, message):
        # Pass 3 contract: destinations receive already-serialized content.
        if self.fail:
            raise RuntimeError("destination unreachable")
        self.sent.append(message.content)


class StubEnricher:
    """Attaches a lookup value so a mapper can reference lookups.<name>.<field>."""

    def __init__(self, name="ref", data=None):
        self.name = name
        self.data = data or {"doctor": "DR HOUSE"}

    def enrich_batch(self, envelopes):
        for env in envelopes:
            env.lookups[self.name] = self.data
        return envelopes


def _mkqueue(tmp_path) -> PersistentQueue:
    return PersistentQueue(str(tmp_path / "r.db"))


def _row(queue, trace_id):
    with queue._get_conn() as conn:
        return conn.execute("SELECT * FROM queue WHERE trace_id = ?", (trace_id,)).fetchone()


VALID_JSON = (
    '{"metadata": {"format": "json"},'
    ' "patient": {"identifiers": [{"system": "urn:mrn", "value": "42", "type": "MRN"}], "name": "Ada"},'
    ' "observations": [{"code": {"system": "http://loinc.org", "value": "4544-3"}, "value": 140.0}]}'
)


def test_json_to_canonical_to_json_delivered(tmp_path):
    q = _mkqueue(tmp_path)
    dest = RecordingDestination()
    runner = ChannelRunner("c1", q, destination=dest, inbound_codec="json", outbound_codec="json")
    env = Envelope(channel_id="c1", raw=VALID_JSON, inbound_codec="json")
    q.enqueue(env)

    assert runner.process_one() is True
    assert dest.sent and len(dest.sent) == 1
    roundtrip = json.loads(dest.sent[0])
    assert roundtrip["patient"]["name"] == "Ada"

    row = _row(q, env.trace_id)
    assert row["state"] == "DELIVERED"
    assert row["attempts"] == 1


def test_invalid_canonical_is_typed_validation_failure(tmp_path):
    q = _mkqueue(tmp_path)
    runner = ChannelRunner("c1", q, destination=RecordingDestination(),
                           inbound_codec="json", outbound_codec="json")
    env = Envelope(channel_id="c1", raw='{"patient": {"dob": "not-a-date"}}', inbound_codec="json")
    q.enqueue(env)

    runner.process_one()
    row = _row(q, env.trace_id)
    assert row["state"] == "DEAD_LETTER"
    err = json.loads(row["error"])
    assert err["stage"] == "validation"
    assert err["code"] == "canonical.invalid"
    assert err["trace_id"] == env.trace_id
    assert err["channel_id"] == "c1"
    assert err["message"]
    assert err["traceback"]


def test_transform_failure_is_typed(tmp_path):
    q = _mkqueue(tmp_path)
    mapper = FieldMapper([{"source": "patient.missing", "target": "x", "required": True}])
    runner = ChannelRunner("c1", q, mapper=mapper, destination=RecordingDestination(),
                           inbound_codec="json", outbound_codec="json")
    env = Envelope(channel_id="c1", raw=VALID_JSON, inbound_codec="json")
    q.enqueue(env)

    runner.process_one()
    row = _row(q, env.trace_id)
    assert row["state"] == "DEAD_LETTER"
    err = json.loads(row["error"])
    assert err["stage"] == "transform"
    assert err["code"] == "transform.failed"


def test_post_transform_validation_failure(tmp_path):
    q = _mkqueue(tmp_path)
    # target "extensions" must be a dict; feeding a scalar makes output invalid
    mapper = FieldMapper([{"source": "patient.name", "target": "extensions"}])
    runner = ChannelRunner("c1", q, mapper=mapper, destination=RecordingDestination(),
                           inbound_codec="json", outbound_codec="json")
    env = Envelope(channel_id="c1", raw=VALID_JSON, inbound_codec="json")
    q.enqueue(env)

    runner.process_one()
    row = _row(q, env.trace_id)
    assert row["state"] == "DEAD_LETTER"
    err = json.loads(row["error"])
    assert err["stage"] == "validation"
    assert err["code"] == "canonical.post_transform"


def test_destination_failure_is_retryable_then_dlq(tmp_path):
    q = _mkqueue(tmp_path)
    runner = ChannelRunner("c1", q, destination=RecordingDestination(fail=True),
                           inbound_codec="json", outbound_codec="json",
                           max_retries=3, base_backoff=0)
    env = Envelope(channel_id="c1", raw=VALID_JSON, inbound_codec="json")
    q.enqueue(env)

    runner.process_one()  # attempt 1 -> retryable -> QUEUED
    row = _row(q, env.trace_id)
    assert row["state"] == "QUEUED"
    assert row["attempts"] == 1
    assert json.loads(row["error"])["stage"] == "destination"

    runner.process_one()  # attempt 2 -> retryable -> QUEUED
    row = _row(q, env.trace_id)
    assert row["state"] == "QUEUED"
    assert row["attempts"] == 2

    runner.process_one()  # attempt 3 -> budget exhausted -> DEAD_LETTER
    row = _row(q, env.trace_id)
    assert row["state"] == "DEAD_LETTER"
    assert row["attempts"] == 3
    assert json.loads(row["error"])["stage"] == "destination"


def test_unknown_inbound_codec_fails_explicitly(tmp_path):
    q = _mkqueue(tmp_path)
    runner = ChannelRunner("c1", q, destination=RecordingDestination(),
                           inbound_codec="does-not-exist", outbound_codec="json")
    env = Envelope(channel_id="c1", raw=VALID_JSON, inbound_codec="does-not-exist")
    q.enqueue(env)

    runner.process_one()
    row = _row(q, env.trace_id)
    assert row["state"] == "DEAD_LETTER"
    err = json.loads(row["error"])
    assert err["stage"] == "decode"
    assert err["code"] == "codec.not_found"


def test_unknown_outbound_codec_fails_explicitly(tmp_path):
    q = _mkqueue(tmp_path)
    runner = ChannelRunner("c1", q, destination=RecordingDestination(),
                           inbound_codec="json", outbound_codec="nope")
    env = Envelope(channel_id="c1", raw=VALID_JSON, inbound_codec="json")
    q.enqueue(env)

    runner.process_one()
    row = _row(q, env.trace_id)
    assert row["state"] == "DEAD_LETTER"
    err = json.loads(row["error"])
    assert err["stage"] == "serialize"
    assert err["code"] == "codec.not_found"


def test_enrichment_and_mapper_run_within_canonical_pipeline(tmp_path):
    q = _mkqueue(tmp_path)
    mapper = FieldMapper([{"source": "lookups.ref.doctor", "target": "x"}])
    runner = ChannelRunner("c1", q, mapper=mapper, enricher=StubEnricher(),
                           destination=RecordingDestination(),
                           inbound_codec="json", outbound_codec="json")
    env = Envelope(channel_id="c1", raw=VALID_JSON, inbound_codec="json")
    q.enqueue(env)

    assert runner.process_one() is True
    row = _row(q, env.trace_id)
    assert row["state"] == "DELIVERED"