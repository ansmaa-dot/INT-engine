"""Typed pipeline error taxonomy.

Each pipeline stage raises a ``PipelineError`` subclass carrying a stable
``code`` and the ``stage`` it occurred in, so the audit/error system can
distinguish between:

    wire        — malformed/invalid at the transport boundary
    decode      — wire data parsed but the codec couldn't produce canonical
    validation  — decoded data violates the canonical model contract
    business    — semantically invalid business data
    transform   — mapping/transform step failed
    serialize   — canonical message couldn't be expressed as target format
    destination — transport/delivery failed

Retry/DLQ policy is deliberately NOT designed here (that is Pass 2). This
pass only defines the taxonomy, stable codes, and error context.
"""
from __future__ import annotations

from typing import Any


class PipelineError(Exception):
    """Base class for all pipeline errors."""

    stage: str = "unknown"
    code: str = "error"

    def __init__(
        self,
        code: str,
        message: str,
        *,
        context: dict[str, Any] | None = None,
        cause: BaseException | None = None,
    ):
        super().__init__(message)
        self.code = code
        self.context = dict(context or {})
        self.cause = cause

    def __str__(self) -> str:
        return f"[{self.stage}/{self.code}] {super().__str__()}"


class WireError(PipelineError):
    stage = "wire"


class DecodeError(PipelineError):
    stage = "decode"


class CanonicalValidationError(PipelineError):
    stage = "validation"


class BusinessValidationError(PipelineError):
    stage = "business"


class TransformError(PipelineError):
    stage = "transform"


class EnrichmentError(PipelineError):
    stage = "enrichment"


class SerializeError(PipelineError):
    stage = "serialize"


class DestinationError(PipelineError):
    stage = "destination"
