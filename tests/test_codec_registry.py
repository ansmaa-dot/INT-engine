import pytest

import nodes.codec  # noqa: F401  -- registers the built-in codecs
from nodes.codec.base import Codec
from nodes.codec.fhir.codec import FhirR4Codec
from nodes.codec.hl7v2.codec import Hl7V2Codec
from nodes.codec.json import JsonCodec
from nodes.codec.passthrough import PassthroughCodec
from nodes.codec.registry import CodecNotFoundError, REGISTRY, get, keys, register


def test_builtin_codecs_registered():
    ks = set(keys())
    assert "json" in ks
    assert "passthrough" in ks
    assert "hl7v2.5.1.ORU_R01" in ks
    assert "hl7v2.5.1.ADT_A01" in ks
    assert "fhir.r4" in ks


def test_get_returns_typed_codec():
    assert isinstance(get("json"), JsonCodec)
    assert isinstance(get("json"), Codec)
    assert isinstance(get("passthrough"), PassthroughCodec)
    assert isinstance(get("hl7v2.5.1.ORU_R01"), Hl7V2Codec)
    assert isinstance(get("hl7v2.5.1.ADT_A01"), Hl7V2Codec)
    assert isinstance(get("fhir.r4"), FhirR4Codec)


def test_profile_codecs_carry_their_profile():
    assert get("hl7v2.5.1.ORU_R01").profile == "ORU_R01"
    assert get("hl7v2.5.1.ADT_A01").profile == "ADT_A01"


def test_unknown_key_raises():
    with pytest.raises(CodecNotFoundError):
        get("hl7v2")
    with pytest.raises(CodecNotFoundError):
        get("fhir")
    with pytest.raises(CodecNotFoundError):
        get("hl7v2.5.1")


def test_unknown_key_never_falls_back():
    # a typo / different casing must not silently resolve to a default codec
    with pytest.raises(CodecNotFoundError):
        get("JSON ")
    with pytest.raises(CodecNotFoundError):
        get("json__")
    with pytest.raises(CodecNotFoundError):
        get("")


def test_not_found_error_carries_key_and_message():
    with pytest.raises(CodecNotFoundError) as ei:
        get("does-not-exist")
    assert ei.value.key == "does-not-exist"
    assert "does-not-exist" in str(ei.value)


def test_manual_register_and_override():
    class MyCodec(Codec):
        key = "_test_only_my"

        def parse(self, raw, metadata=None):
            raise NotImplementedError

        def serialize(self, canonical):
            raise NotImplementedError

    register(MyCodec())
    try:
        assert isinstance(get("_test_only_my"), MyCodec)
    finally:
        REGISTRY.pop("_test_only_my", None)

    assert "_test_only_my" not in keys()