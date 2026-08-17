import traceback

from core.queue import PersistentQueue
from core.message import MessageState


class ChannelRunner:
    def __init__(
        self,
        channel_id: str,
        queue: PersistentQueue,
        mapper,
        destination=None,
        enricher=None,
        max_retries: int = 3,
        base_backoff: int = 2,
    ):
        self.channel_id = channel_id
        self.queue = queue
        self.mapper = mapper
        self.destination = destination
        self.enricher = enricher  # Case 2: optional BatchLookup, run before mapping
        self.max_retries = max_retries
        self.base_backoff = base_backoff  # Initial backoff in seconds (e.g., 2s, 4s, 8s)

    def process_one(self) -> bool:
        envelope = self.queue.dequeue_available(self.channel_id)
        if not envelope:
            return False

        envelope.attempts += 1
        self.queue.record_audit(envelope.trace_id, self.channel_id, "processing_started",
                                 {"attempt": envelope.attempts})

        try:
            # 1. Enrichment (Case 2) — attaches reference data to envelope.lookups.
            #    Single-message here since process_one drains one row at a time;
            #    a higher-throughput batch drain loop would call
            #    enricher.enrich_batch() once per drained batch instead.
            if self.enricher:
                self.enricher.enrich_batch([envelope])

            # 2. Execute mapping / transformation pipeline (Case 3)
            transformed = self.mapper.transform(envelope.raw_payload, envelope.lookups)

            # 3. Dispatch to destination protocol (Case 4)
            if self.destination:
                self.destination.send(transformed)

            # 4. Mark delivered on success
            envelope.transformed_payload = transformed
            envelope.state = MessageState.DELIVERED
            self.queue.mark_delivered(envelope.trace_id, transformed)
            return True

        except Exception as e:
            error_msg = str(e)
            # Keep the message short (surfaces in tables) but persist the full
            # stack trace too, so the dashboard's Destination pane can show
            # exactly where processing failed (Mirth-style error inspection).
            error_trace = traceback.format_exc()
            envelope.last_error = error_msg

            if envelope.attempts < self.max_retries:
                delay = self.base_backoff * (2 ** (envelope.attempts - 1))
                envelope.state = MessageState.QUEUED
                self.queue.mark_retry(
                    envelope.trace_id, envelope.attempts, delay, error_msg,
                    traceback_str=error_trace,
                )
            else:
                envelope.state = MessageState.DEAD_LETTER
                self.queue.mark_dead_letter(
                    envelope.trace_id, envelope.attempts, error_msg,
                    traceback_str=error_trace,
                )

            return True
