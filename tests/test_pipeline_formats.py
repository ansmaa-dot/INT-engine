"""Pass 5 end-to-end cross-format tests.

These prove the two new codecs inter-operate through the canonical model and
through the real engine: an HL7 v2 result decoded, mapped, and re-emitted as a
FHIR R4 bundle (and the reverse), plus a full ChannelRunner delivery.
"""

import json

import nodes.codec  # noqa: F401 -- registers built-in codecs
from core.message import Envelope
from core.queue import PersistentQueue
from engine.runner import ChannelRunner
from nodes.codec.registry import get
from tests.fixtures import fhir_bundle_json, oru_r01

HL7 = get("hl7v2.5.1.ORU_R01")
FHIR = get("fhir.r4")


def test_hl7_to_fhir_bundle():
    canonical = HL7.parse(oru_r01())
    out = FHIR.serialize(canonical)
    bundle = json.loads(out)
    assert bundle["resourceType"] == "Bundle"
    assert bundle["id"] == "CTRL001"  # provenance follows the message

    types = {r["resource"]["resourceType"] for r in bundle["entry"]}
    assert types == {"Patient", "ServiceRequest", "Specimen", "Observation"}

    patient = next(r["resource"] for r in bundle["entry"]
                   if r["resource"]["resourceType"] == "Patient")
    # PID-5 family^given normalizes to "given family" when both are present.
    assert patient["name"][0]["text"] == "Jane Doe"
    assert patient["birthDate"] == "1990-01-02"

    sr = next(r["resource"] for r in bundle["entry"]
              if r["resource"]["resourceType"] == "ServiceRequest")
    assert sr["accessionNumber"] == "FILLER-200"

    spec = next(r["resource"] for r in bundle["entry"]
                if r["resource"]["resourceType"] == "Specimen")
    assert spec["type"]["text"] == "Whole Blood"

    # The FHIR bundle round-trips back into an equivalent canonical message.
    back = FHIR.parse(out)
    assert back.patient.name == "Jane Doe"
    assert back.observations[0].value == 140
    assert back.observations[0].extensions.get("abnormal_flags") == ["H"]


def test_fhir_to_hl7_message():
    canonical = FHIR.parse(fhir_bundle_json())
    out = HL7.serialize(canonical)
    assert out.startswith("MSH")
    assert "OBX" in out

    back = HL7.parse(out)
    assert back.patient.name == "Doe^Jane"
    assert back.patient.dob.isoformat() == "1990-01-02"
    assert back.order.accession.value == "FILLER-200"
    assert back.order.priority == "routine"
    assert back.observations[0].value == 140
    assert back.observations[0].extensions.get("abnormal_flags") == ["H"]


class RecordingDestination:
    def __init__(self):
        self.sent = []

    def send(self, message):
        self.sent.append(message.content)


def test_runner_hl7_inbound_fhir_outbound(tmp_path):
    q = PersistentQueue(str(tmp_path / "p5.db"))
    dest = RecordingDestination()
    runner = ChannelRunner(
        "p5", q, destination=dest,
        inbound_codec="hl7v2.5.1.ORU_R01", outbound_codec="fhir.r4",
    )
    env = Envelope(channel_id="p5", raw=oru_r01(), inbound_codec="hl7v2.5.1.ORU_R01")
    q.enqueue(env)

    assert runner.process_one() is True
    assert len(dest.sent) == 1
    bundle = json.loads(dest.sent[0])
    assert bundle["resourceType"] == "Bundle"
    assert any(r["resource"]["resourceType"] == "Patient" for r in bundle["entry"])

    with q._get_conn() as conn:
        row = conn.execute("SELECT state FROM queue WHERE trace_id=?", (env.trace_id,)).fetchone()
    assert row["state"] == "DELIVERED"