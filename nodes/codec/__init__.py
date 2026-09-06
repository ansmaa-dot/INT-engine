"""Codec package.

Importing this package registers the built-in codecs in
``nodes.codec.registry``.
"""
from nodes.codec import registry
from nodes.codec.base import Codec
from nodes.codec.json import JsonCodec
from nodes.codec.passthrough import PassthroughCodec
from nodes.codec.hl7v2.codec import Hl7V2Codec
from nodes.codec.fhir.codec import FhirR4Codec
from nodes.codec.rawjson import SchemalessJsonCodec
from nodes.codec.registry import (
    CodecNotFoundError,
    get,
    keys,
    register,
    structure,
)

# Register built-in codecs on import (all registration is explicit).
register(JsonCodec())
register(PassthroughCodec())
register(SchemalessJsonCodec())
register(Hl7V2Codec(profile="ORU_R01", key="hl7v2.5.1.ORU_R01"))
register(Hl7V2Codec(profile="ADT_A01", key="hl7v2.5.1.ADT_A01"))
register(Hl7V2Codec(profile="ORM_O01", key="hl7v2.5.1.ORM_O01"))
register(Hl7V2Codec(profile="UNDEFINED", key="hl7v2.5.1.UNDEFINED"))
register(FhirR4Codec(key="fhir.r4"))

__all__ = [
    "Codec",
    "CodecNotFoundError",
    "FhirR4Codec",
    "Hl7V2Codec",
    "JsonCodec",
    "PassthroughCodec",
    "SchemalessJsonCodec",
    "get",
    "keys",
    "register",
    "structure",
]