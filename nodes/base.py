from abc import ABC, abstractmethod
from typing import Any

class BaseNode(ABC):
    """Abstract base class for all integration pipeline nodes."""
    pass


class IngestionNode(BaseNode):
    """Base class for all message ingestion sources (listeners or pollers)."""
    @abstractmethod
    def start(self, on_message_callback) -> None:
        """Starts the ingestion source, sending raw messages to the callback."""
        pass

    @abstractmethod
    def stop(self) -> None:
        """Stops the ingestion source."""
        pass


class EnrichmentNode(BaseNode):
    """Base class for reference-data lookup / batch enrichment nodes."""
    @abstractmethod
    def enrich_batch(self, envelopes: list) -> list:
        """Enriches a batch of Envelopes in one round trip, attaching
        results to env.lookups[<lookup_name>]. Returns the same list."""
        pass


class TransformNode(BaseNode):
    """Base class for all payload transformation / mapping nodes."""
    @abstractmethod
    def transform(self, data: Any) -> Any:
        """Transforms input data and returns the mapped output."""
        pass


class DestinationNode(BaseNode):
    """Base class for all outbound message delivery protocols."""
    @abstractmethod
    def send(self, payload: Any) -> Any:
        """Dispatches the payload to the external destination."""
        pass
