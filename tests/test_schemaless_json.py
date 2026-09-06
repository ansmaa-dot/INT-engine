"""Schemaless JSON codec tests.

Covers the shape-normalization boundary: flat dot-notation payloads from
db_poller / webhook-style sinks unflatten into a nested tree, canonical groups
populate the model, unknown inbound keys survive under ``extensions.*``, and a
schemaless channel correctly populates outbound HL7 PID (the original silent-
failure mode).
"""

import json

import pytest

import nodes.codec  # noqa: F401 -- registers built-in codecs
from core.errors import CanonicalValidationError, DecodeError
from nodes.codec.registry import get

COD = get("schemaless.json")

FLAT = json.dumps({
    # canonical leaves, flat dot-notation (db_poller row / flat webhook)
    "patient.name": "John Doe",
    "patient.identifiers.0.value": "123",
    "patient.identifiers.0.system": "urn:mrn",
    "patient.identifiers.0.type": "MRN",
    "patient.dob": "1970-06-15",
    "patient.gender": "M",
    "order.accession.value": "ACC-1",
    "order.accession.system": "urn:acc",
    "order.accession.type": "FILLER",
    "observations.0.code.value": "4544-3",
    "observations.0.code.system": "http://loinc.org",
    "observations.0.value": 140,
    "observations.0.unit": "mg/dL",
    # arbitrary, non-canonical inbound data (preserved under extensions)
    "unmapped_db_column": "something",
    "vendor.flag.key": "preserved",
})


def test_codec_is_registered_schemaless():
    assert COD.key == "schemaless.json"
    assert COD.structure == "schemaless"


def test_flat_dot_notation_populates_nested_model():
    c = COD.parse(FLAT)
    assert c.patient is not None
    assert c.patient.name == "John Doe"
    assert c.patient.gender == "M"
    assert c.patient.dob.isoformat() == "1970-06-15"
    assert c.patient.identifiers[0].value == "123"
    assert c.patient.identifiers[0].system == "urn:mrn"
    assert c.patient.identifiers[0].type == "MRN"

    assert c.order is not None
    assert c.order.accession is not None
    assert c.order.accession.value == "ACC-1"

    assert len(c.observations) == 1
    assert c.observations[0].code.value == "4544-3"
    assert c.observations[0].value == 140
    assert c.observations[0].unit == "mg/dL"


def test_non_canonical_keys_preserved_under_extensions():
    c = COD.parse(FLAT)
    assert c.extensions["unmapped_db_column"] == "something"
    assert c.extensions["vendor"]["flag"]["key"] == "preserved"


def test_already_nested_payload_passes_through():
    nested = {"patient": {"name": "Ada", "identifiers": [{"value": "42"}]}}
    c = COD.parse(json.dumps(nested))
    assert c.patient.name == "Ada"
    assert c.patient.identifiers[0].value == "42"
    assert c.extensions == {}


def test_explicit_extensions_key_merges_with_projected_keys():
    c = COD.parse(json.dumps({
        "extensions": {"explicit": 1},
        "vendor.flag": 2,
    }))
    assert c.extensions["explicit"] == 1
    assert c.extensions["vendor"]["flag"] == 2


def test_parse_accepts_bytes():
    c = COD.parse(json.dumps({"patient.identifiers.0.value": "77"}).encode("utf-8"))
    assert c.patient is not None
    assert c.patient.identifiers[0].value == "77"


def test_invalid_json_raises_decode_error():
    with pytest.raises(DecodeError) as ei:
        COD.parse("{not json")
    assert ei.value.code == "json.invalid"


def test_non_object_json_raises_decode_error():
    with pytest.raises(DecodeError) as ei:
        COD.parse("[1, 2, 3]")
    assert ei.value.code == "json.not_object"


def test_bad_canonical_leaf_still_fails_validation():
    with pytest.raises(CanonicalValidationError):
        COD.parse(json.dumps({"patient": {"dob": "not-a-date"}}))


def test_serialize_produces_canonical_json():
    c = COD.parse(FLAT)
    out = json.loads(COD.serialize(c))
    assert out["patient"]["name"] == "John Doe"
    assert out["patient"]["identifiers"][0]["value"] == "123"
    # preserved arbitrary inbound data survives the round trip
    assert out["extensions"]["unmapped_db_column"] == "something"


def test_metadata_filled_from_wire_context():
    from core.wire import WireContext

    c = COD.parse(FLAT, WireContext(source="src", message_id="mid-1"))
    assert c.metadata.source == "src"
    assert c.metadata.message_id == "mid-1"
    assert c.metadata.format == "schemaless.json"


def test_payload_own_metadata_wins_over_wire_context():
    from core.wire import WireContext

    c = COD.parse(
        json.dumps({"patient.name": "X", "metadata": {"format": "custom"}}),
        WireContext(source="src"),
    )
    assert c.metadata.format == "custom"
    assert c.metadata.source is None  # untouched by context


def test_via_runner_to_hl7_pid_populated(tmp_path):
    """The original bug end-to-end: a flat schemaless payload must populate
    outbound HL7 PID instead of silently producing empty segments."""
    from core.message import Envelope
    from core.queue import PersistentQueue
    from engine.runner import ChannelRunner

    class RecordingDestination:
        def __init__(self):
            self.sent = []

        def send(self, message):
            self.sent.append(message.content)

    q = PersistentQueue(str(tmp_path / "schemaless.db"))
    dest = RecordingDestination()
    runner = ChannelRunner(
        "c1", q, destination=dest,
        inbound_codec="schemaless.json", outbound_codec="hl7v2.5.1.ORU_R01",
    )
    env = Envelope(channel_id="c1", raw=FLAT, inbound_codec="schemaless.json")
    q.enqueue(env)

    assert runner.process_one() is True
    wire = dest.sent[0]
    assert "John Doe" in wire  # PID-5 populated from patient.name
    assert "123^^^urn:mrn^MRN" in wire  # PID-3 identifier
    assert "19700615" in wire  # PID-7 dob
    assert "ACC-1" in wire  # OBR-3 filler / accession