"""Pass 5 HL7 v2 codec tests: parse the golden ORU/ADT fixtures, verify every
mapping, round-trip the canonical model, exercise escaping and malformed input.
"""

import pytest

import nodes.codec  # noqa: F401 -- registers built-in codecs
from core.errors import DecodeError
from core.model import CanonicalMessage, Identifier, Order, PatientSummary, Specimen
from nodes.codec.hl7v2.codec import Hl7V2Codec
from nodes.codec.registry import get
from tests.fixtures import adt_a01, oru_r01

ORU = Hl7V2Codec(profile="ORU_R01", key="hl7v2.5.1.ORU_R01-test")
ADT = get("hl7v2.5.1.ADT_A01")


def test_parse_oru_golden_metadata_and_patient():
    c = ORU.parse(oru_r01())
    assert c.metadata.format == "hl7v2"
    assert c.metadata.version == "2.5.1"
    assert c.metadata.message_type == "ORU"
    assert c.metadata.message_id == "CTRL001"

    p = c.patient
    assert p is not None
    assert p.name == "Jane Doe"  # XPN family^given is normalized to "given family"
    assert p.dob.isoformat() == "1990-01-02"
    assert p.gender == "F"
    # PID-3 carried two identifiers via a '~' repetition.
    assert [(i.value, i.system, i.type) for i in p.identifiers] == [
        ("12345", "HOSP", "MRN"),
        ("67890", "SOC", "SSN"),
    ]


def test_parse_oru_golden_encounter_order_specimen():
    c = ORU.parse(oru_r01())

    assert c.encounter is not None
    assert c.encounter.visit_number.value == "VISIT999"

    o = c.order
    assert o is not None
    assert any(i.type == "PLACER" and i.value == "PLACER-100" for i in o.identifiers)
    assert any(i.type == "FILLER" and i.value == "FILLER-200" for i in o.identifiers)
    assert o.accession is not None and o.accession.value == "FILLER-200"
    assert o.requested_at.isoformat() == "2024-01-01T10:30:00"
    assert o.priority == "routine"
    assert o.ordering_provider is not None and o.ordering_provider.value == "DR100"
    assert o.items[0].code.value == "CBC"
    assert o.items[0].code.system == "http://loinc.org"

    assert len(c.specimen) == 1
    s = c.specimen[0]
    assert s.identifiers[0].value == "SPEC-500"
    assert s.type == "Whole Blood"
    assert s.collected_at.isoformat() == "2024-01-01T09:00:00"


def test_parse_oru_golden_observations():
    c = ORU.parse(oru_r01())
    assert len(c.observations) == 2

    g = c.observations[0]
    assert g.code.value == "4544-3"
    assert g.code.system == "http://loinc.org"
    assert g.value == 140
    assert g.unit == "mg/dL"
    assert g.reference_range == "70-100"
    assert g.status == "final"
    assert g.observed_at.isoformat() == "2024-01-01T10:30:00"
    # OBX-8 abnormal flags -> Observation.extensions (never dropped).
    assert g.extensions.get("abnormal_flags") == ["H"]

    h = c.observations[1]
    assert h.code.value == "718-7"
    assert h.value == 15.2
    assert h.status == "final"
    assert "abnormal_flags" not in h.extensions


def test_parse_adt_a01():
    c = ADT.parse(adt_a01())
    assert c.metadata.message_type == "ADT"
    assert c.patient.identifiers[0].value == "555000111"
    assert c.encounter.visit_number.value == "VISIT123"


def test_round_trip_hl7_numeric_and_escaped_values():
    # A name containing '^' and special characters must survive escape/unescape.
    canonical = CanonicalMessage(
        patient=PatientSummary(
            identifiers=[Identifier(value="12345", system="HOSP", type="MRN")],
            name="Doe^Jane|MD",
            dob=__import__("datetime").date(1990, 1, 2),
            gender="F",
        ),
    )
    text = ORU.serialize(canonical)
    assert "\\F\\" in text  # the '^' was escaped
    assert "\\S\\" in text  # the '|' was escaped as \S\
    back = ORU.parse(text)
    assert back.patient.name == "Doe^Jane|MD"


def test_canonical_to_hl7_to_canonical_round_trip():
    from datetime import date, datetime

    canonical = CanonicalMessage()
    canonical.metadata.message_id = "M1"
    canonical.metadata.version = "2.5.1"
    canonical.patient = PatientSummary(
        identifiers=[Identifier(value="12345", system="HOSP", type="MRN")],
        name="Doe^Jane", dob=date(1990, 1, 2), gender="F",
    )
    canonical.order = Order(
        identifiers=[Identifier(value="PLACER-100", system="LAB", type="PLACER")],
        accession=Identifier(value="FILLER-200", system="LAB", type="FILLER"),
        requested_at=datetime(2024, 1, 1, 10, 30),
        priority="routine",
    )
    canonical.specimen = [Specimen(identifiers=[Identifier(value="SPEC-500", type="SPECIMEN")],
                                   type="Whole Blood", collected_at=datetime(2024, 1, 1, 9))]

    text = ORU.serialize(canonical)
    back = ORU.parse(text)
    assert back.patient.identifiers[0].value == "12345"
    assert back.patient.name == "Doe^Jane"
    assert back.order.accession.value == "FILLER-200"
    assert back.order.priority == "routine"
    assert back.specimen[0].type == "Whole Blood"
    assert back.specimen[0].collected_at.isoformat() == "2024-01-01T09:00:00"


def test_malformed_raises_typed_error():
    with pytest.raises(DecodeError) as ei:
        ORU.parse("this is not an HL7 message at all")
    assert ei.value.code == "hl7.no_msh"


def test_missing_msh_raises():
    with pytest.raises(DecodeError) as ei:
        ORU.parse("PID|1||12345||Doe^Jane")
    assert ei.value.code in ("hl7.no_msh", "hl7.malformed")