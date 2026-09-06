"""Codec contract: a wire format ↔ CanonicalMessage bridge.

Complex formats should hide their parser/serializer internals *behind* this
interface — callers only ever see ``parse()``/``serialize()`` and the
``CanonicalMessage`` they produce/consume. A codec is responsible for
transcoding the wire format; it is NOT responsible for semantic validation
of the business content (that is a separate pipeline stage).
"""
from __future__ import annotations

from abc import ABC, abstractmethod

from core.model import CanonicalMessage
from core.wire import WireContext


class Codec(ABC):
    """Translates between a wire format and the canonical message.

    Subclasses MUST set ``key`` (unique registry key, per format/version/
    profile where relevant) and implement ``parse``/``serialize``.
    """

    key: str = ""

    #: Wire-format shape contract. ``"structured"`` codecs (HL7/FHIR/canonical
    #: JSON) have a fixed schema their ``parse()`` resolves into a nested tree
    #: by format rules. ``"schemaless"`` codecs (db_poller/http_json/webhook raw
    #: JSON) accept dynamic payloads and must normalize the shape inside
    #: ``parse()`` (e.g. unflatten dot-notation keys) before validating into the
    #: canonical model. Stored on the registry for config-time transport↔codec
    #: shape guidance; it is metadata, never a behavior fork by itself.
    structure: str = "structured"

    @abstractmethod
    def parse(
        self, raw: str | bytes, metadata: WireContext | None = None
    ) -> CanonicalMessage:
        """Decode wire data into a validated ``CanonicalMessage``.

        ``metadata`` is transport context (source, message id, ...); it is
        not required. Raises ``DecodeError`` / ``CanonicalValidationError``
        on failure.
        """
        raise NotImplementedError

    @abstractmethod
    def serialize(self, canonical: CanonicalMessage) -> str:
        """Encode a ``CanonicalMessage`` back into wire text.

        Raises ``SerializeError`` if the message can't be expressed as the
        target format.
        """
        raise NotImplementedError