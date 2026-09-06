"""Pass 5 FHIR R4 codec tests: golden bundle parse, canonical round-trip,
priority/status inversion, lossless abnormal-flag handling, malformed input."""

import json
from datetime import date, datetime

import pytest

import nodes.codec  # noqa: F401 -- registers built-in codecs
from core.errors import DecodeError
from core.model import (
    CanonicalMessage,
    Identifier,
    Observation,
    Order,
    OrderItem,
    PatientSummary,
    Specimen,
)
from nodes.codec.fhir.codec import FhirR4Codec
from tests.fixtures import fhir_bundle_dict, fhir_bundle_json

FH = FhirR4Codec(key="fhir.r4")


def _canonical() -> CanonicalMessage:
    c = CanonicalMessage()
    c.metadata.format = "fhir-r4"
    c.metadata.message_id = "M1"
    c.patient = PatientSummary(
        identifiers=[Identifier(value="12345", system="HOSP", type="MRN")],
        name="Doe^Jane", dob=date(1990, 1, 2), gender="F",
    )
    c.order = Order(
        identifiers=[Identifier(value="PLACER-100", type="PLACER")],
        accession=Identifier(value="FILLER-200", type="FILLER"),
        requested_at=datetime(2024, 1, 1, 10, 30),
        priority="routine",
        items=[OrderItem(code=Identifier(value="4544-3", system="http://loinc.org"))],
    )
    c.specimen = [Specimen(identifiers=[Identifier(value="SPEC-500", type="SPECIMEN")],
                           type="Whole Blood", collected_at=datetime(2024, 1, 1, 9))]
    c.observations = [
        Observation(code=Identifier(value="4544-3", system="http://loinc.org"),
                    status="final", value=140, unit="mg/dL",
                    reference_range="70-100",
                    observed_at=datetime(2024, 1, 1, 10, 30),
                    extensions={"abnormal_flags": ["H"]}),
        Observation(code=Identifier(value="718-7", system="http://loinc.org"),
                    status="preliminary", value=15.2, unit="g/dL",
                    reference_range="12-16"),
    ]
    return c


def test_serialize_produces_bundle_structure():
    bundle = json.loads(FH.serialize(_canonical()))
    assert bundle["resourceType"] == "Bundle"
    assert bundle["id"] == "M1"
    types = {r["resource"]["resourceType"] for r in bundle["entry"]}
    assert types == {"Patient", "ServiceRequest", "Specimen", "Observation"}

    sr = next(r["resource"] for r in bundle["entry"]
              if r["resource"]["resourceType"] == "ServiceRequest")
    assert sr["accessionNumber"] == "FILLER-200"
    assert sr["priority"] == "routine"
    obs = next(r["resource"] for r in bundle["entry"]
               if r["resource"]["resourceType"] == "Observation")
    assert obs["valueQuantity"] == {"value": 140, "unit": "mg/dL"}
    assert obs["interpretation"] == [{"coding": [{"code": "H"}]}]


def test_round_trip_canonical_fhir_canonical():
    src = _canonical()
    back = FH.parse(FH.serialize(src))
    assert src.model_dump(mode="json") == back.model_dump(mode="json")


def test_parse_golden_bundle():
    c = FH.parse(fhir_bundle_json())
    assert c.metadata.format == "fhir-r4"
    assert c.metadata.message_id == "CTRL001"
    assert c.patient.name == "Doe^Jane"
    assert c.patient.dob.isoformat() == "1990-01-02"
    assert c.patient.gender == "F"
    assert c.patient.identifiers[0].value == "12345"
    assert c.patient.identifiers[0].system == "HOSP"
    assert c.patient.identifiers[0].type == "MRN"

    assert c.order.accession.value == "FILLER-200"
    assert c.order.priority == "routine"
    assert c.order.requested_at.isoformat() == "2024-01-01T10:30:00"
    assert c.order.items[0].code.value == "4544-3"

    assert len(c.specimen) == 1
    assert c.specimen[0].type == "Whole Blood"

    assert len(c.observations) == 1
    o = c.observations[0]
    assert o.value == 140
    assert o.unit == "mg/dL"
    assert o.reference_range == "70-100"
    assert o.status == "final"
    assert o.extensions.get("abnormal_flags") == ["H"]


def test_abnormal_flags_never_dropped():
    b = fhir_bundle_dict()
    c = FH.parse(json.dumps(b))
    assert c.observations[0].extensions.get("abnormal_flags") == ["H"]
    # and they map back onto interpretation on serialize
    out = json.loads(FH.serialize(c))
    obs = next(r["resource"] for r in out["entry"]
               if r["resource"]["resourceType"] == "Observation")
    assert obs["interpretation"][0]["coding"][0]["code"] == "H"


def test_priority_inversion_preoperative():
    c = _canonical()
    c.order.priority = "preoperative"
    out = json.loads(FH.serialize(c))
    sr = next(r["resource"] for r in out["entry"]
              if r["resource"]["resourceType"] == "ServiceRequest")
    assert sr["priority"] == "urgent"
    back = FH.parse(json.dumps(out))
    assert back.order.priority == "preoperative"


def test_status_inversion_unable_to_obtain():
    c = _canonical()
    c.observations[0].status = "unable_to_obtain"
    out = json.loads(FH.serialize(c))
    obs = next(r["resource"] for r in out["entry"]
               if r["resource"]["resourceType"] == "Observation")
    assert obs["status"] == "unknown"
    back = FH.parse(json.dumps(out))
    assert back.observations[0].status == "unable_to_obtain"


def test_not_a_bundle_raises():
    with pytest.raises(DecodeError) as ei:
        FH.parse('{"resourceType": "Patient", "id": "x"}')
    assert ei.value.code == "fhir.not_bundle"


def test_invalid_json_raises():
    with pytest.raises(DecodeError) as ei:
        FH.parse("this is {not json")
    assert ei.value.code == "fhir.invalid_json"