import json

import pytest

from core.errors import CanonicalValidationError, DecodeError
from core.model import CanonicalMessage
from nodes.codec.base import Codec
from nodes.codec.json import JsonCodec

COD = JsonCodec()


def _original() -> CanonicalMessage:
    return CanonicalMessage(
        metadata={"format": "json", "message_id": "m1"},
        patient={
            "identifiers": [
                {"system": "urn:mrn", "value": "42", "type": "MRN"}
            ],
            "name": "Ada",
        },
        observations=[
            {"code": {"system": "http://loinc.org", "value": "4544-3"}, "value": 140.0}
        ],
    )


def test_normalized_round_trip():
    original = _original()
    wire = COD.serialize(original)
    parsed = COD.parse(wire)
    # canonical -> serialize -> parse -> normalized canonical is equal
    assert parsed == original
    assert parsed.model_dump(mode="json") == original.model_dump(mode="json")


def test_parse_accepts_json_string():
    parsed = COD.parse('{"patient": {"name": "Grace"}}')
    assert parsed.patient.name == "Grace"


def test_parse_accepts_bytes():
    parsed = COD.parse(b'{"observations": [{"value": 7}]}')
    assert parsed.observations[0].value == 7


def test_serialize_produces_valid_json_object():
    data = json.loads(COD.serialize(CanonicalMessage()))
    assert isinstance(data, dict)
    assert data["schema_version"] == "1.0"


def test_invalid_json_raises_decode_error():
    with pytest.raises(DecodeError) as ei:
        COD.parse("{not json")
    assert ei.value.stage == "decode"
    assert ei.value.code == "json.invalid"


def test_non_object_json_raises_decode_error():
    with pytest.raises(DecodeError) as ei:
        COD.parse("[1, 2, 3]")
    assert ei.value.code == "json.not_object"


def test_undecodable_bytes_raises_decode_error():
    with pytest.raises(DecodeError) as ei:
        COD.parse(b"\xff\xfe\x00\x01")
    assert ei.value.code == "json.undecodable"


def test_valid_json_invalid_model_raises_validation_error():
    with pytest.raises(CanonicalValidationError) as ei:
        COD.parse('{"patient": {"dob": "not-a-date"}}')
    assert ei.value.stage == "validation"
    assert ei.value.code == "canonical.invalid"


def test_valid_json_wrong_schema_version_raises_validation_error():
    with pytest.raises(CanonicalValidationError):
        COD.parse('{"schema_version": "9.9"}')


def test_metadata_survives_round_trip():
    original = _original()
    parsed = COD.parse(COD.serialize(original))
    assert parsed.metadata.format == "json"
    assert parsed.metadata.message_id == "m1"


def test_extensions_round_trip():
    original = CanonicalMessage(extensions={"vendor": {"x": [1, 2]}})
    parsed = COD.parse(COD.serialize(original))
    assert parsed.extensions == {"vendor": {"x": [1, 2]}}


def test_codec_is_abstract_contract():
    assert isinstance(COD, Codec)
    assert COD.key == "json"