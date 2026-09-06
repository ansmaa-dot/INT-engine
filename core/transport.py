"""Transport and destination message contracts (Pass 3).

Enforces two clean boundaries:

    * Inbound:  Transport -> raw bytes/text + minimal metadata
    * Outbound: Codec -> serialized bytes/text -> Destination

Transports must NOT understand business/message formats — they surface raw
content plus the small amount of routing/provenance metadata the engine needs
(source, an optional source-id/idempotency hint, filename, content type).
Decoding to CanonicalMessage is a codec concern handled later in the pipeline
(the runner's decode stage).

Destinations receive already-serialized content (str/bytes), never arbitrary
dicts — serialization is the outbound codec's job, performed before the
destination is reached.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from core.message import Envelope


@dataclass
class TransportMessage:
    """Raw content produced by an inbound transport, plus minimal metadata.

    Transports never interpret the content; they only frame/collect it.
    """

    raw: str | bytes
    source: str | None = None        # channel/source identifier
    message_id: str | None = None    # optional source-id / idempotency hint
    filename: str | None = None
    content_type: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class DestinationMessage:
    """Already-serialized content handed to a destination, plus delivery
    metadata. Destinations never serialize — the content is final."""

    content: str | bytes
    content_type: str | None = None
    filename: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


def to_envelope(
    channel_id: str, msg: TransportMessage, inbound_codec: str = "json"
) -> Envelope:
    """Wrap a transport message as an Envelope for the persistent queue.

    Only the raw content, the inbound codec, and the optional source-id
    idempotency hint cross into the envelope — the transport never dictates
    message semantics.
    """
    raw = msg.raw
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    env = Envelope(channel_id=channel_id, raw=raw, inbound_codec=inbound_codec)
    if msg.message_id:
        env.idempotency_key = msg.message_id
    return env
