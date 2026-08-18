"""HL7 v2.5.1 codec (ORU_R01 / ADT_A01 / ORM_O01): Codec implementation.

Deliberately thin: delegates to the parser/serializer modules and adapts them
to the engine's Codec contract. All HL7 structure handling stays inside this
package — the MLLP transport only frames bytes.

Profile validation: the codec checks that the incoming MSH-9 message type
matches the expected profile. A mismatched message (e.g. ADT on an ORU
channel) is rejected with a DecodeError rather than silently producing a
partially-populated CanonicalMessage.
"""
from __future__ import annotations

from core.errors import DecodeError, SerializeError
from core.model import CanonicalMessage
from core.wire import WireContext
from nodes.codec.base import Codec
from nodes.codec.hl7v2.parser import parse_to_canonical
from nodes.codec.hl7v2.serializer import serialize_message

# Map codec profile -> expected MSH-9 message type (type^event).
_PROFILE_MSH9 = {
    "ORU_R01": "ORU^R01",
    "ADT_A01": "ADT^A01",
    "ORM_O01": "ORM^O01",
    "UNDEFINED": None,  # accept any message type
}


class Hl7V2Codec(Codec):
    """HL7 v2.5.1 codec for a specific message profile."""

    def __init__(self, profile: str, key: str, version: str = "2.5.1"):
        self.profile = profile
        self.version = version
        self.key = key

    def parse(self, raw, metadata: WireContext | None = None) -> CanonicalMessage:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        canonical = parse_to_canonical(raw, metadata)

        # Validate that the incoming message type matches the codec's profile.
        # UNDEFINED profile (expected=None) skips validation — accepts any type.
        expected = _PROFILE_MSH9.get(self.profile, ...)
        if expected is not ... and expected is not None and canonical.metadata is not None:
            actual = canonical.metadata.message_type
            if actual and actual != expected:
                raise DecodeError(
                    "hl7.profile_mismatch",
                    f"expected {expected}, received {actual} "
                    f"(codec profile is {self.profile})",
                )
        return canonical

    def serialize(self, canonical: CanonicalMessage) -> str:
        try:
            return serialize_message(canonical, profile=self.profile)
        except (TypeError, ValueError) as e:
            raise SerializeError(
                "hl7.serialize", f"could not serialize canonical to HL7: {e}",
                cause=e,
            ) from e
