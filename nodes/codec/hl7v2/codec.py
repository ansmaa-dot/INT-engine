"""HL7 v2.5.1 codec (ORU_R01 / ADT_A01): Codec implementation.

Deliberately thin: delegates to the parser/serializer modules and adapts them
to the engine's Codec contract. All HL7 structure handling stays inside this
package — the MLLP transport only frames bytes.
"""
from __future__ import annotations

from core.errors import SerializeError
from core.model import CanonicalMessage
from core.wire import WireContext
from nodes.codec.base import Codec
from nodes.codec.hl7v2.parser import parse_to_canonical
from nodes.codec.hl7v2.serializer import serialize_message


class Hl7V2Codec(Codec):
    """HL7 v2.5.1 codec for a specific message profile."""

    def __init__(self, profile: str, key: str, version: str = "2.5.1"):
        self.profile = profile
        self.version = version
        self.key = key

    def parse(self, raw, metadata: WireContext | None = None) -> CanonicalMessage:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        return parse_to_canonical(raw, metadata)

    def serialize(self, canonical: CanonicalMessage) -> str:
        try:
            return serialize_message(canonical, profile=self.profile)
        except (TypeError, ValueError) as e:
            raise SerializeError(
                "hl7.serialize", f"could not serialize canonical to HL7: {e}",
                cause=e,
            ) from e
