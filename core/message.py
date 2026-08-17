import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional
import uuid


class MessageState(Enum):
    QUEUED = "QUEUED"
    PROCESSING = "PROCESSING"
    DELIVERED = "DELIVERED"
    DEAD_LETTER = "DEAD_LETTER"


@dataclass
class Envelope:
    channel_id: str
    raw_payload: Dict[str, Any]
    trace_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    state: MessageState = MessageState.QUEUED
    attempts: int = 0
    transformed_payload: Optional[Dict[str, Any]] = None
    lookups: Dict[str, Any] = field(default_factory=dict)
    idempotency_key: Optional[str] = None
    last_error: Optional[str] = None
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    next_retry_at: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict) -> "Envelope":
        """Reconstructs an Envelope instance from a database row dictionary."""
        # Convert state string back into MessageState Enum safely
        state_val = data.get("state", "QUEUED")
        try:
            state_enum = MessageState[state_val.upper()]
        except KeyError:
            state_enum = MessageState.QUEUED

        # Deserialize JSON payloads stored as SQLite TEXT strings
        raw_payload = data.get("raw_payload")
        if isinstance(raw_payload, str):
            raw_payload = json.loads(raw_payload)

        transformed_payload = data.get("transformed_payload")
        if isinstance(transformed_payload, str) and transformed_payload:
            transformed_payload = json.loads(transformed_payload)

        return cls(
            trace_id=data["trace_id"],
            channel_id=data["channel_id"],
            state=state_enum,
            attempts=data.get("attempts", 0),
            raw_payload=raw_payload or {},
            transformed_payload=transformed_payload,
            last_error=data.get("last_error"),
            created_at=data.get("created_at"),
            next_retry_at=data.get("next_retry_at"),
            idempotency_key=data.get("idempotency_key"),
        )