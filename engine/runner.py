"""ChannelRunner: ordered pipeline step-chain executor.

A channel's ``pipeline`` is an ordered list of typed ``Step`` objects
(``engine.steps``) interpreted here in array order:

    decode -> validate -> [ enrich | transform | filter | assert ]*
           -> validate -> encode -> deliver

Step semantics (plan §1 / §3 D5, D6, D10):

- ``enrich``    — read-only BatchLookup; failures are permanent.
- ``transform`` — pure FieldMapper; output re-validated as CanonicalMessage.
- ``filter``    — boolean expression; ``false`` routes per ``on_fail``:
                  ``dead_letter`` (DLQ, never retried) or ``discard``
                  (terminal DISCARDED state, distinct from DLQ).
- ``assert``    — boolean expression; ``false`` routes per ``on_fail``:
                  ``retry`` (retryable) or ``dead_letter``.

Retry/permanence is driven per step via the error's ``retryable`` flag;
the global ``PERMANENT_STAGES`` set is only the fallback for the fixed
decode/validate/encode/deliver phases.
"""
import traceback
from dataclasses import dataclass, field

from pydantic import ValidationError as PydanticValidationError

from core.queue import PersistentQueue
from core.message import Envelope, MessageState
from core.model import CanonicalMessage
from core.errors import (
    AssertError,
    CanonicalValidationError,
    DecodeError,
    DestinationError,
    EnrichmentError,
    FilterError,
    PipelineError,
    SerializeError,
    TransformError,
)
from core.expression import ExpressionError, evaluate
from core.wire import WireContext
from core.transport import DestinationMessage
from engine.steps import Step
from nodes.codec import get as get_codec
from nodes.codec.registry import CodecNotFoundError

# Fallback stages whose failures are treated as permanent (no retry — DLQ
# directly) when an error carries no explicit ``retryable`` flag. Step-chain
# errors always carry one (plan §3 D5).
PERMANENT_STAGES = {"decode", "validation", "business", "enrichment", "transform", "serialize"}


@dataclass
class DryRunResult:
    """Outcome of a side-effect-free ``run_dry`` execution."""

    ok: bool = True
    steps: list[dict] = field(default_factory=list)
    error: dict | None = None
    wire: str | None = None


class ChannelRunner:
    def __init__(
        self,
        channel_id: str,
        queue: PersistentQueue,
        steps: list[Step] | None = None,
        destination=None,
        inbound_codec: str = "json",
        outbound_codec: str = "json",
        max_retries: int = 3,
        base_backoff: int = 2,
    ):
        self.channel_id = channel_id
        self.queue = queue
        self.steps = list(steps or [])
        self.destination = destination
        self.inbound_codec = inbound_codec
        self.outbound_codec = outbound_codec
        self.max_retries = max_retries
        self.base_backoff = base_backoff  # Initial backoff in seconds (e.g., 2s, 4s, 8s)


    def process_one(self) -> bool:
        """Runs the canonical pipeline for one message (dequeue, execute,
        success/failure bookkeeping). Returns True if a message was
        processed — including when it failed — and False when the queue
        was empty."""
        envelope = self.queue.dequeue_available(self.channel_id)
        if not envelope:
            return False

        envelope.attempts += 1
        self.queue.record_audit(envelope.trace_id, self.channel_id, "processing_started",
                                 {"attempt": envelope.attempts})

        try:
            self.process_envelope(envelope)
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

    def process_envelope(self, envelope: Envelope, *, deliver: bool = True,
                         destination=None) -> str:
        """Pure pipeline core — no queue mutation: decode → validate →
        ordered step chain → post-validate → encode → (deliver if enabled).
        Raises the typed ``PipelineError`` on failure; mutates
        ``envelope.canonical`` / ``envelope.lookups`` in place and returns
        the encoded wire form."""
        canonical = self._decode(envelope)
        self._validate(canonical)
        envelope.canonical = canonical

        # Ordered step chain: each step sees the current canonical message.
        # ``_run_step`` returns a replacement CanonicalMessage (transform) or
        # None (enrich / passing filter / passing assert).
        for step in self.steps:
            outcome = self._run_step(step, envelope)
            if outcome is not None:
                envelope.canonical = outcome

        # Validate post-chain: transform steps may have reshaped the message.
        self._validate(envelope.canonical)

        wire = self._encode(envelope.canonical)
        if deliver:
            self._deliver(wire, destination=destination)
        return wire

    def run_dry(self, envelope: Envelope) -> DryRunResult:
        """Side-effect-free execution for testing/preview: no delivery, no
        queue mutation, and the caller's envelope is left untouched (a deep
        copy is staged). Enrich steps still perform their read-only lookups
        so the result reflects what would really happen."""
        import copy

        dry_env = copy.deepcopy(envelope)
        dry_env.attempts = 1
        result = DryRunResult()

        try:
            canonical = self._decode(dry_env)
            self._validate(canonical)
            dry_env.canonical = canonical

            for step in self.steps:
                outcome = self._run_step(step, dry_env)
                if outcome is not None:
                    dry_env.canonical = outcome
                result.steps.append(
                    {"step_id": step.step_id, "type": step.type, "outcome": "ok"})

            self._validate(dry_env.canonical)
            result.wire = self._encode(dry_env.canonical)
            return result
        except PipelineError as e:
            result.ok = False
            result.error = {
                "stage": e.stage,
                "code": e.code,
                "message": str(e),
                "step_id": e.step_id,
                "step_type": e.step_type,
                "action": (e.context or {}).get("action"),
            }
            return result

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

    # --- step chain --------------------------------------------------------

    def _run_step(self, step: Step, envelope: Envelope):
        """Executes one pipeline step. Returns the replacement
        ``CanonicalMessage`` for transform steps, or ``None`` when the
        canonical message is unchanged (enrich / passing filter or assert).
        Routing decisions (filter/assert false) raise typed errors carrying
        the step's identity."""
        try:
            if step.type == "enrich":
                step.impl.enrich_batch([envelope])
                return None

            if step.type == "transform":
                mapped = step.impl.transform(envelope.canonical_dict, envelope.lookups)
                return CanonicalMessage.model_validate(mapped)

            # filter / assert — evaluate the expression over the canonical
            # JSON form plus the runtime lookups namespace.
            data = dict(envelope.canonical_dict)
            data["lookups"] = envelope.lookups or {}
            ok = bool(evaluate(step.impl, data))
            if ok:
                return None

            if step.type == "filter":
                action = "discard" if step.on_fail == "discard" else "dead_letter"
                raise FilterError(
                    "filter.evaluated_false",
                    f"filter step {step.step_id!r} evaluated false "
                    f"(on_fail={step.on_fail})",
                    context={"action": action},
                    step_id=step.step_id,
                    step_type=step.type,
                    retryable=False,
                )
            raise AssertError(
                "assert.evaluated_false",
                f"assert step {step.step_id!r} evaluated false "
                f"(on_fail={step.on_fail})",
                step_id=step.step_id,
                step_type=step.type,
                retryable=(step.on_fail == "retry"),
            )

        except PipelineError:
            raise
        except ExpressionError as e:
            cls = FilterError if step.type == "filter" else AssertError
            raise cls(
                f"{step.type}.invalid_expression",
                f"{step.type} step {step.step_id!r} has an invalid expression: {e}",
                cause=e,
                step_id=step.step_id,
                step_type=step.type,
                retryable=False,
            ) from e
        except PydanticValidationError as e:
            raise CanonicalValidationError(
                "canonical.post_transform",
                f"transformed output of step {step.step_id!r} is not a valid "
                f"canonical message: {e}",
                cause=e,
                step_id=step.step_id,
                step_type=step.type,
            ) from e
        except Exception as e:
            if step.type == "enrich":
                raise EnrichmentError(
                    "enrichment.failed",
                    f"enrichment step {step.step_id!r} failed: {e}",
                    cause=e,
                    step_id=step.step_id,
                    step_type=step.type,
                ) from e
            raise TransformError(
                "transform.failed",
                f"transform step {step.step_id!r} failed: {e}",
                cause=e,
                step_id=step.step_id,
                step_type=step.type,
            ) from e

    # --- fixed phases ------------------------------------------------------

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

    def _deliver(self, wire: str, destination=None) -> None:
        dest = destination if destination is not None else self.destination
        if not dest:
            return
        # The destination receives already-serialized content in a
        # DestinationMessage — it never sees a dict or does its own encoding.
        message = DestinationMessage(content=wire)
        try:
            dest.send(message)
        except PipelineError:
            raise
        except Exception as e:
            raise DestinationError(
                "destination.failed", f"delivery failed: {e}", cause=e
            ) from e

    # --- failure classification -------------------------------------------

    def _handle_failure(self, envelope, exc: BaseException, tb: str) -> None:
        """Records the failure with stage/code/message/traceback and routes
        it. Step-chain errors carry an explicit ``retryable`` flag (per-step
        on_fail, plan §3 D5); without one, the permanent-stage set is the
        fallback for the fixed phases. Filter discards are a terminal,
        observable state — never DLQ, never retried (D6/D9)."""
        error = {
            "stage": getattr(exc, "stage", "unknown"),
            "code": getattr(exc, "code", "error"),
            "message": str(exc),
            "traceback": tb,
            "trace_id": envelope.trace_id,
            "channel_id": self.channel_id,
        }
        step_id = getattr(exc, "step_id", None)
        step_type = getattr(exc, "step_type", None)
        if step_id is not None:
            error["step_id"] = step_id
        if step_type is not None:
            error["step_type"] = step_type
        envelope.error = error

        context = getattr(exc, "context", None) or {}
        if getattr(exc, "stage", "") == "filter" and context.get("action") == "discard":
            envelope.state = MessageState.DISCARDED
            self.queue.mark_discarded(envelope.trace_id, envelope.attempts, error)
            return

        retryable = getattr(exc, "retryable", None)
        if retryable is None:
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
