"""Transport/runtime context handed to a codec's ``parse()``.

This is deliberately separate from the domain ``CanonicalMessage``: it
describes *how* the wire data arrived (source, message id, content type,
receive time), not the healthcare content. Keep it minimal — add a field only
when a real transport actually needs to communicate it.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass
class WireContext:
    source: str | None = None
    message_id: str | None = None
    received_at: datetime | None = None
    content_type: str | None = None
