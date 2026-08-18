from datetime import date, datetime, timezone

import pytest
from pydantic import ValidationError

from core.model import (
    CanonicalMessage,
    Encounter,
    Identifier,
    MessageMetadata,
    Observation,
    Order,
    OrderItem,
    PatientSummary,
    Specimen,
)


def _sample() -> CanonicalMessage:
    return CanonicalMessage(
        metadata=MessageMetadata(format="json", version="1", message_id="msg-1"),
        patient=PatientSummary(
            identifiers=[
                Identifier(system="urn:test:mrn", value="12345", type="MRN")
            ],
            name="Jane Doe",
            dob=date(1990, 1, 2),
            gender="female",
        ),
        order=Order(
            accession=Identifier(
                system="urn:test:accession", value="ACC-1", type="accession"
            ),
            requested_at=datetime(2024, 5, 1, 10, 30, tzinfo=timezone.utc),
            items=[
                OrderItem(
                    code=Identifier(
                        system="http://loinc.org", value="4544-3", type="LOINC"
                    )
                )
            ],
        ),
        specimen=[Specimen(identifiers=[Identifier(value="S-1")], type="blood")],
        observations=[
            Observation(
                code=Identifier(system="http://loinc.org", value="4544-3"),
                value=140.0,
                unit="mg/dL",
            )
        ],
        extensions={"vendor_field": {"foo": 1}},
    )


def test_empty_message_uses_schema_version_and_defaults():
    c = CanonicalMessage()
    assert c.schema_version == "1.0"
    assert c.patient is None
    assert c.encounter is None
    assert c.order is None
    assert c.specimen == []
    assert c.observations == []
    assert c.metadata == MessageMetadata()
    assert c.extensions == {}


def test_sample_populates_fields():
    c = _sample()
    assert c.patient.identifiers[0].value == "12345"
    assert c.patient.dob == date(1990, 1, 2)
    assert c.patient.gender == "female"
    assert c.order.accession.value == "ACC-1"
    assert c.order.items[0].code.value == "4544-3"
    assert c.specimen[0].type == "blood"
    assert c.observations[0].value == 140.0
    assert c.observations[0].unit == "mg/dL"


def test_schema_version_is_pinned_literal():
    with pytest.raises(ValidationError):
        CanonicalMessage(schema_version="2.0")


def test_identifier_requires_value():
    with pytest.raises(ValidationError):
        Identifier(system="urn:x")


def test_model_dump_json_round_trips_through_validation():
    c = _sample()
    data = c.model_dump(mode="json")
    assert CanonicalMessage.model_validate(data) == c


def test_empty_list_defaults_are_distinct_instances():
    a = CanonicalMessage()
    a.specimen.append(Specimen())
    assert CanonicalMessage().specimen == []


def test_dates_serialize_to_iso_strings_in_json_mode():
    c = _sample()
    data = c.model_dump(mode="json")
    assert data["patient"]["dob"] == "1990-01-02"
    assert data["order"]["requested_at"].startswith("2024-05-01")