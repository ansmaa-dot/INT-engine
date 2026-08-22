"""Typed pipeline error taxonomy.

Each pipeline stage raises a ``PipelineError`` subclass carrying a stable
``code`` and the ``stage`` it occurred in, so the audit/error system can
distinguish between:

    wire        — malformed/invalid at the transport boundary
    decode      — wire data parsed but the codec couldn't produce canonical
    validation  — decoded data violates the canonical model contract
    business    — semantically invalid business data
    transform   — mapping/transform step failed
    filter      — filter-step boolean expression evaluated to false
    assert      — assert-step boolean expression evaluated to false
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
        step_id: str | None = None,
        step_type: str | None = None,
        retryable: bool | None = None,
    ):
        super().__init__(message)
        self.code = code
        self.step_id = step_id
        self.step_type = step_type
        self.retryable = retryable

        # Merge step identity into context for audit/error serialization.
        merged = dict(context or {})
        if step_id is not None:
            merged.setdefault("step_id", step_id)
        if step_type is not None:
            merged.setdefault("step_type", step_type)
        self.context = merged

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


class FilterError(PipelineError):
    """Raised when a filter step's boolean expression evaluates to false.

    The ``on_fail`` action (dead_letter or discard) is set by the caller
    and determines how the runner handles this error. The error itself is
    always non-retryable by default — the filter made a clean decision,
    there is nothing to retry.
    """

    stage = "filter"


class AssertError(PipelineError):
    """Raised when an assert step's boolean expression evaluates to false.

    Retryability is determined by the step's ``on_fail`` setting:
      - ``on_fail=retry`` → retryable (transient condition)
      - ``on_fail=dead_letter`` → not retryable (persistent failure)
    """

    stage = "assert"


class SerializeError(PipelineError):
    stage = "serialize"


class DestinationError(PipelineError):
    stage = "destination"
