import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional
import uuid

from core.model import CanonicalMessage


class MessageState(Enum):
    QUEUED = "QUEUED"
    PROCESSING = "PROCESSING"
    DELIVERED = "DELIVERED"
    DEAD_LETTER = "DEAD_LETTER"


@dataclass
class Envelope:
    """The message envelope carried between the queue and the pipeline.

    This is deliberately NOT payload-centric. It carries:
      * ``raw``            — the original wire message (kept for audit/debug,
                             never used as the pipeline's working currency)
      * ``inbound_codec``  — codec key used to decode ``raw`` -> canonical
      * ``canonical``      — the CanonicalMessage, the only currency between
                             decode and encode
      * ``lookups``        — enrichment results attached by the enrichment stage
      * trace/message identifiers and lifecycle state

    There is intentionally NO ``raw_payload`` field — the engine operates on
    ``CanonicalMessage``, never on a transport-specific dict shape.
    """
    channel_id: str
    raw: Optional[str] = None
    inbound_codec: str = "json"
    canonical: Optional[CanonicalMessage] = None
    lookups: Dict[str, Any] = field(default_factory=dict)
    trace_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    state: MessageState = MessageState.QUEUED
    attempts: int = 0
    error: Optional[Dict[str, Any]] = None
    idempotency_key: Optional[str] = None
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    next_retry_at: Optional[str] = None

    @property
    def canonical_dict(self) -> dict:
        """JSON-safe dict view of the canonical message, for the existing
        dict-based enrichment/mapper steps. Empty dict when no canonical is
        present yet."""
        if self.canonical is None:
            return {}
        return self.canonical.model_dump(mode="json")

    @classmethod
    def from_dict(cls, data: dict) -> "Envelope":
        """Reconstructs an Envelope instance from a database row dictionary."""
        state_val = data.get("state", "QUEUED")
        try:
            state_enum = MessageState[state_val.upper()]
        except KeyError:
            state_enum = MessageState.QUEUED

        # Deserialize canonical (stored as JSON TEXT) back into the model.
        canonical = data.get("canonical")
        if isinstance(canonical, str) and canonical:
            canonical = CanonicalMessage.model_validate_json(canonical)
        elif canonical is None:
            canonical = None
        else:
            canonical = CanonicalMessage.model_validate(canonical)

        # Deserialize the structured error (JSON TEXT) into a dict.
        error = data.get("error")
        if isinstance(error, str):
            try:
                error = json.loads(error) or None
            except (TypeError, ValueError):
                error = None

        return cls(
            trace_id=data["trace_id"],
            channel_id=data["channel_id"],
            state=state_enum,
            attempts=data.get("attempts", 0),
            raw=data.get("raw"),
            inbound_codec=data.get("inbound_codec") or "json",
            canonical=canonical,
            error=error,
            idempotency_key=data.get("idempotency_key"),
            created_at=data.get("created_at"),
            next_retry_at=data.get("next_retry_at"),
        )