import json

from core.message import Envelope
from core.queue import PersistentQueue
from engine.runner import ChannelRunner
from engine.steps import Step, build_step
from nodes.base import EnrichmentNode
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


class StubEnricher(EnrichmentNode):
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
    steps = [build_step({"step_id": "t1", "type": "transform",
                         "config": {"rules": [{"source": "patient.missing",
                                               "target": "x", "required": True}]}})]
    runner = ChannelRunner("c1", q, steps=steps, destination=RecordingDestination(),
                           inbound_codec="json", outbound_codec="json")
    env = Envelope(channel_id="c1", raw=VALID_JSON, inbound_codec="json")
    q.enqueue(env)

    runner.process_one()
    row = _row(q, env.trace_id)
    assert row["state"] == "DEAD_LETTER"
    err = json.loads(row["error"])
    assert err["stage"] == "transform"
    assert err["code"] == "transform.failed"
    assert err["step_id"] == "t1"
    assert err["step_type"] == "transform"


def test_post_transform_validation_failure(tmp_path):
    q = _mkqueue(tmp_path)
    # target "extensions" must be a dict; feeding a scalar makes output invalid
    steps = [build_step({"step_id": "t1", "type": "transform",
                         "config": {"rules": [{"source": "patient.name",
                                               "target": "extensions"}]}})]
    runner = ChannelRunner("c1", q, steps=steps, destination=RecordingDestination(),
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
    steps = [
        Step(step_id="e1", type="enrich", config={}, impl=StubEnricher()),
        build_step({"step_id": "t1", "type": "transform",
                    "config": {"rules": [{"source": "lookups.ref.doctor",
                                          "target": "x"}]}}),
    ]
    runner = ChannelRunner("c1", q, steps=steps, destination=RecordingDestination(),
                           inbound_codec="json", outbound_codec="json")
    env = Envelope(channel_id="c1", raw=VALID_JSON, inbound_codec="json")
    q.enqueue(env)

    assert runner.process_one() is True
    row = _row(q, env.trace_id)
    assert row["state"] == "DELIVERED"


# ---------------------------------------------------------------------------
# step-chain semantics: filter
# ---------------------------------------------------------------------------


def test_filter_false_discards_message(tmp_path):
    q = _mkqueue(tmp_path)
    steps = [build_step({"step_id": "f1", "type": "filter", "config": {
        "expression": {"==": [{"var": ["patient.name"]}, "nobody"]},
        "on_fail": "discard"}})]
    runner = ChannelRunner("c1", q, steps=steps, destination=RecordingDestination(),
                           inbound_codec="json", outbound_codec="json",
                           max_retries=3, base_backoff=0)
    env = Envelope(channel_id="c1", raw=VALID_JSON, inbound_codec="json")
    q.enqueue(env)

    runner.process_one()
    row = _row(q, env.trace_id)
    assert row["state"] == "DISCARDED"  # terminal; never DLQ, never retried
    err = json.loads(row["error"])
    assert err["stage"] == "filter"
    assert err["code"] == "filter.evaluated_false"
    assert err["step_id"] == "f1"
    assert err["step_type"] == "filter"
    # discarded messages leave the work queue
    assert runner.process_one() is False


def test_filter_false_dead_letters_without_retry(tmp_path):
    q = _mkqueue(tmp_path)
    steps = [build_step({"step_id": "f1", "type": "filter", "config": {
        "expression": {"==": [{"var": ["patient.name"]}, "nobody"]},
        "on_fail": "dead_letter"}})]
    runner = ChannelRunner("c1", q, steps=steps, destination=RecordingDestination(),
                           inbound_codec="json", outbound_codec="json",
                           max_retries=3, base_backoff=0)
    env = Envelope(channel_id="c1", raw=VALID_JSON, inbound_codec="json")
    q.enqueue(env)

    runner.process_one()
    row = _row(q, env.trace_id)
    assert row["state"] == "DEAD_LETTER"
    assert row["attempts"] == 1  # filter DLQ is never retried


def test_filter_true_passes_message_through(tmp_path):
    q = _mkqueue(tmp_path)
    dest = RecordingDestination()
    steps = [build_step({"step_id": "f1", "type": "filter", "config": {
        "expression": {"==": [{"var": ["patient.name"]}, "Ada"]}}})]
    runner = ChannelRunner("c1", q, steps=steps, destination=dest,
                           inbound_codec="json", outbound_codec="json")
    env = Envelope(channel_id="c1", raw=VALID_JSON, inbound_codec="json")
    q.enqueue(env)

    assert runner.process_one() is True
    assert dest.sent  # passing filter does not block delivery
    assert _row(q, env.trace_id)["state"] == "DELIVERED"


# ---------------------------------------------------------------------------
# step-chain semantics: assert
# ---------------------------------------------------------------------------


def test_assert_false_retries_then_dlq(tmp_path):
    q = _mkqueue(tmp_path)
    steps = [build_step({"step_id": "a1", "type": "assert", "config": {
        "expression": {"not_empty": [{"var": ["patient.dob"]}]},
        "on_fail": "retry"}})]
    runner = ChannelRunner("c1", q, steps=steps, destination=RecordingDestination(),
                           inbound_codec="json", outbound_codec="json",
                           max_retries=3, base_backoff=0)
    env = Envelope(channel_id="c1", raw=VALID_JSON, inbound_codec="json")
    q.enqueue(env)

    runner.process_one()  # attempt 1 -> retryable -> QUEUED
    row = _row(q, env.trace_id)
    assert row["state"] == "QUEUED"
    assert row["attempts"] == 1
    err = json.loads(row["error"])
    assert err["stage"] == "assert"
    assert err["step_id"] == "a1"
    assert err["step_type"] == "assert"

    runner.process_one()  # attempt 2 -> retryable -> QUEUED
    assert _row(q, env.trace_id)["state"] == "QUEUED"

    runner.process_one()  # attempt 3 -> budget exhausted -> DEAD_LETTER
    row = _row(q, env.trace_id)
    assert row["state"] == "DEAD_LETTER"
    assert row["attempts"] == 3


def test_assert_false_dead_letters(tmp_path):
    q = _mkqueue(tmp_path)
    steps = [build_step({"step_id": "a1", "type": "assert", "config": {
        "expression": {"not_empty": [{"var": ["patient.dob"]}]},
        "on_fail": "dead_letter"}})]
    runner = ChannelRunner("c1", q, steps=steps, destination=RecordingDestination(),
                           inbound_codec="json", outbound_codec="json",
                           max_retries=3, base_backoff=0)
    env = Envelope(channel_id="c1", raw=VALID_JSON, inbound_codec="json")
    q.enqueue(env)

    runner.process_one()
    row = _row(q, env.trace_id)
    assert row["state"] == "DEAD_LETTER"
    assert row["attempts"] == 1


def test_assert_true_passes_message_through(tmp_path):
    q = _mkqueue(tmp_path)
    dest = RecordingDestination()
    steps = [build_step({"step_id": "a1", "type": "assert", "config": {
        "expression": {"not_empty": [{"var": ["patient.name"]}]}}})]
    runner = ChannelRunner("c1", q, steps=steps, destination=dest,
                           inbound_codec="json", outbound_codec="json")
    env = Envelope(channel_id="c1", raw=VALID_JSON, inbound_codec="json")
    q.enqueue(env)

    assert runner.process_one() is True
    assert dest.sent
    assert _row(q, env.trace_id)["state"] == "DELIVERED"


# ---------------------------------------------------------------------------
# step-chain semantics: run_dry + ordering
# ---------------------------------------------------------------------------


def test_run_dry_is_side_effect_free(tmp_path):
    q = _mkqueue(tmp_path)
    dest = RecordingDestination()
    steps = [build_step({"step_id": "t1", "type": "transform", "config": {
        "rules": [{"source": "patient.name", "target": "patient.name",
                   "fn": "Uppercase"}]}})]
    runner = ChannelRunner("c1", q, steps=steps, destination=dest,
                           inbound_codec="json", outbound_codec="json")
    env = Envelope(channel_id="c1", raw=VALID_JSON, inbound_codec="json")
    q.enqueue(env)

    result = runner.run_dry(env)
    assert result.ok is True
    assert result.steps == [{"step_id": "t1", "type": "transform", "outcome": "ok"}]
    assert json.loads(result.wire)["patient"]["name"] == "ADA"

    # no side effects: nothing delivered, queue untouched, envelope untouched
    assert dest.sent == []
    assert _row(q, env.trace_id)["state"] == "QUEUED"
    assert env.canonical is None


def test_run_dry_reports_step_error(tmp_path):
    q = _mkqueue(tmp_path)
    steps = [build_step({"step_id": "f1", "type": "filter", "config": {
        "expression": {"==": [{"var": ["patient.name"]}, "nobody"]},
        "on_fail": "discard"}})]
    runner = ChannelRunner("c1", q, steps=steps, destination=RecordingDestination(),
                           inbound_codec="json", outbound_codec="json")
    env = Envelope(channel_id="c1", raw=VALID_JSON, inbound_codec="json")

    result = runner.run_dry(env)
    assert result.ok is False
    assert result.error["stage"] == "filter"
    assert result.error["step_id"] == "f1"
    assert result.error["action"] == "discard"
    assert result.wire is None


def test_step_chain_runs_in_order(tmp_path):
    q = _mkqueue(tmp_path)
    dest = RecordingDestination()
    # two transforms chained: uppercase first, then echo the result
    steps = [
        build_step({"step_id": "t1", "type": "transform", "config": {
            "rules": [{"source": "patient.name", "target": "patient.name",
                       "fn": "Uppercase"}]}}),
        build_step({"step_id": "t2", "type": "transform", "config": {
            "rules": [{"source": "patient.name", "target": "extensions.echo"}]}}),
    ]
    runner = ChannelRunner("c1", q, steps=steps, destination=dest,
                           inbound_codec="json", outbound_codec="json")
    env = Envelope(channel_id="c1", raw=VALID_JSON, inbound_codec="json")
    q.enqueue(env)

    assert runner.process_one() is True
    sent = json.loads(dest.sent[0])
    # t2 saw t1's output — array order is the ordering authority
    assert sent["patient"]["name"] == "ADA"
    assert sent["extensions"]["echo"] == "ADA"

