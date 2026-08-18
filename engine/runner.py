import traceback

from pydantic import ValidationError as PydanticValidationError

from core.queue import PersistentQueue
from core.message import MessageState
from core.model import CanonicalMessage
from core.errors import (
    CanonicalValidationError,
    DecodeError,
    DestinationError,
    PipelineError,
    SerializeError,
    TransformError,
)
from core.wire import WireContext
from core.transport import DestinationMessage
from nodes.codec import get as get_codec
from nodes.codec.registry import CodecNotFoundError

# Stages whose failures are treated as permanent (no retry — DLQ directly).
PERMANENT_STAGES = {"decode", "validation", "business", "transform", "serialize"}


class ChannelRunner:
    def __init__(
        self,
        channel_id: str,
        queue: PersistentQueue,
        mapper=None,
        destination=None,
        enricher=None,
        inbound_codec: str = "json",
        outbound_codec: str = "json",
        max_retries: int = 3,
        base_backoff: int = 2,
    ):
        self.channel_id = channel_id
        self.queue = queue
        self.mapper = mapper
        self.destination = destination
        self.enricher = enricher  # Case 2: optional BatchLookup, run after decode
        self.inbound_codec = inbound_codec
        self.outbound_codec = outbound_codec
        self.max_retries = max_retries
        self.base_backoff = base_backoff  # Initial backoff in seconds (e.g., 2s, 4s, 8s)

    def process_one(self) -> bool:
        """Runs the canonical pipeline for one message:

            decode -> validate canonical -> enrichment -> transform
                   -> validate post-transform -> encode -> destination

        The engine operates on CanonicalMessage from decode through encode;
        nothing between those boundaries depends on a transport-specific
        payload shape. Errors are recorded per-stage and classified:
        parsing/validation/transform/serialization failures are permanent
        (DLQ), destination failures (and anything unexpected) are retryable.
        """
        envelope = self.queue.dequeue_available(self.channel_id)
        if not envelope:
            return False

        envelope.attempts += 1
        self.queue.record_audit(envelope.trace_id, self.channel_id, "processing_started",
                                 {"attempt": envelope.attempts})

        try:
            # 1. Decode: wire -> CanonicalMessage
            canonical = self._decode(envelope)

            # 2. Canonical schema validation
            self._validate(canonical)
            envelope.canonical = canonical

            # 3. Enrichment (optional) — attaches reference data to envelope.lookups
            self._enrich(envelope)

            # 4. Transformation (optional) — existing dict-based mapper,
            #    fed from the canonical model and validated back into it
            mapped = self._transform(envelope)

            # 5. Validate post-transform
            self._validate(mapped)
            envelope.canonical = mapped

            # 6. Encode: CanonicalMessage -> wire
            wire = self._encode(mapped)

            # 7. Destination (transport only — receives the serialized wire)
            self._deliver(wire)

            envelope.state = MessageState.DELIVERED
            self.queue.mark_delivered(envelope.trace_id, envelope.canonical, envelope.attempts)
            return True

        except PipelineError as e:
            self._handle_failure(envelope, e, traceback.format_exc())
            return True
        except Exception as e:
            # Anything not explicitly classified (unexpected) is treated as
            # retryable rather than assumed permanent.
            unexpected = PipelineError("unknown.error", f"unexpected failure: {e}", cause=e)
            self._handle_failure(envelope, unexpected, traceback.format_exc())
            return True

    def _decode(self, envelope) -> CanonicalMessage:
        try:
            codec = get_codec(envelope.inbound_codec)
        except CodecNotFoundError as e:
            raise DecodeError(
                "codec.not_found",
                f"inbound codec {envelope.inbound_codec!r} not found",
                cause=e,
            ) from e
        try:
            # Pass transport/runtime context along with the raw message so a
            # codec can populate provenance (source, message id) in metadata.
            meta = WireContext(source=self.channel_id, message_id=envelope.trace_id)
            return codec.parse(envelope.raw, meta)
        except PipelineError:
            raise
        except Exception as e:
            raise DecodeError("decode.failed", f"decode failed: {e}", cause=e) from e

    def _validate(self, canonical: CanonicalMessage) -> None:
        """Canonical schema validation. Pydantic already enforces the model
        contract on construction; this is the explicit stage boundary where
        additional schema invariants will live in later passes."""
        if not isinstance(canonical, CanonicalMessage):
            raise CanonicalValidationError(
                "canonical.type", "pipeline expected a CanonicalMessage"
            )

    def _enrich(self, envelope) -> None:
        if not self.enricher:
            return
        try:
            self.enricher.enrich_batch([envelope])
        except PipelineError:
            raise
        except Exception as e:
            raise TransformError(
                "enrichment.failed", f"enrichment failed: {e}", cause=e
            ) from e

    def _transform(self, envelope) -> CanonicalMessage:
        """Runs the existing FieldMapper over the canonical message's JSON
        form, then validates the mapped output back into a CanonicalMessage.
        With no mapper configured the canonical message passes through."""
        if not self.mapper:
            return envelope.canonical
        try:
            data = envelope.canonical_dict
            mapped = self.mapper.transform(data, envelope.lookups)
            return CanonicalMessage.model_validate(mapped)
        except PydanticValidationError as e:
            raise CanonicalValidationError(
                "canonical.post_transform",
                f"transformed output is not a valid canonical message: {e}",
                cause=e,
            ) from e
        except PipelineError:
            raise
        except Exception as e:
            raise TransformError(
                "transform.failed", f"transform failed: {e}", cause=e
            ) from e

    def _encode(self, canonical: CanonicalMessage) -> str:
        try:
            codec = get_codec(self.outbound_codec)
        except CodecNotFoundError as e:
            raise SerializeError(
                "codec.not_found",
                f"outbound codec {self.outbound_codec!r} not found",
                cause=e,
            ) from e
        try:
            return codec.serialize(canonical)
        except PipelineError:
            raise
        except Exception as e:
            raise SerializeError(
                "encode.failed", f"encode failed: {e}", cause=e
            ) from e

    def _deliver(self, wire: str) -> None:
        if not self.destination:
            return
        # The destination receives already-serialized content in a
        # DestinationMessage — it never sees a dict or does its own encoding.
        message = DestinationMessage(content=wire)
        try:
            self.destination.send(message)
        except PipelineError:
            raise
        except Exception as e:
            raise DestinationError(
                "destination.failed", f"delivery failed: {e}", cause=e
            ) from e

    # --- failure classification -------------------------------------------

    def _handle_failure(self, envelope, exc: BaseException, tb: str) -> None:
        """Records the failure with stage/code/message/traceback and routes it:
        permanent stages go straight to DLQ; destination/unknown failures retry
        with exponential backoff until the retry budget is exhausted."""
        error = {
            "stage": getattr(exc, "stage", "unknown"),
            "code": getattr(exc, "code", "error"),
            "message": str(exc),
            "traceback": tb,
            "trace_id": envelope.trace_id,
            "channel_id": self.channel_id,
        }
        envelope.error = error

        retryable = error["stage"] not in PERMANENT_STAGES
        if retryable and envelope.attempts < self.max_retries:
            delay = self.base_backoff * (2 ** (envelope.attempts - 1))
            envelope.state = MessageState.QUEUED
            self.queue.mark_retry(
                envelope.trace_id, envelope.attempts, delay, error
            )
        else:
            envelope.state = MessageState.DEAD_LETTER
            self.queue.mark_dead_letter(
                envelope.trace_id, envelope.attempts, error
            )
