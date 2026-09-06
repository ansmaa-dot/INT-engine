from typing import List

from core.message import Envelope
from core.canonical_paths import resolve_path
from core.errors import EnrichmentError
from nodes.base import EnrichmentNode
from nodes.enrichment.db_adapter import (
    DatabaseAdapter,
    SqliteAdapter,
    is_valid_identifier,
)


class BatchLookup(EnrichmentNode):
    """Resolves reference data (patient name, doctor id, test code...) for a
    whole batch of envelopes in one query, instead of one query per message
    (the N+1 pattern that kills some engine channels under load).

    By default *db_path* creates a ``SqliteAdapter`` (backward-compatible).
    Pass *db_adapter* directly to use PostgreSQL, MySQL, or a custom backend.

    All table/column identifiers are validated against ``is_valid_identifier``
    to prevent SQL injection from misconfigured enrichments.
    """

    def __init__(
        self,
        db_path: str | None = None,
        source_key_field: str = "",
        target_table: str = "",
        target_key_col: str = "",
        fields: List[str] | None = None,
        lookup_name: str = "",
        db_adapter: DatabaseAdapter | None = None,
    ):
        # Validate identifiers before accepting them.
        for label, val in [
            ("target_table", target_table),
            ("target_key_col", target_key_col),
        ]:
            if val and not is_valid_identifier(val):
                raise EnrichmentError(
                    "enrichment.invalid_identifier",
                    f"enrichment {label}={val!r} is not a valid SQL identifier",
                )
        for i, fld in enumerate(fields or []):
            if not is_valid_identifier(fld):
                raise EnrichmentError(
                    "enrichment.invalid_identifier",
                    f"enrichment field[{i}]={fld!r} is not a valid SQL identifier",
                )

        self.db_path = db_path
        self.source_key_field = source_key_field
        self.target_table = target_table
        self.target_key_col = target_key_col
        self.fields = fields or []
        self.lookup_name = lookup_name
        self._adapter = db_adapter

    @property
    def adapter(self) -> DatabaseAdapter:
        if self._adapter is None:
            if not self.db_path:
                raise EnrichmentError(
                    "enrichment.no_db",
                    "BatchLookup requires db_path or db_adapter",
                )
            self._adapter = SqliteAdapter(self.db_path)
        return self._adapter

    def enrich_batch(self, envelopes: List[Envelope]) -> List[Envelope]:
        keys = set()
        for e in envelopes:
            k = self._extract_key(e)
            if k is not None:
                keys.add(k)

        if not keys:
            for env in envelopes:
                env.lookups[self.lookup_name] = None
            return envelopes

        # All identifiers have been validated; safe for f-string interpolation.
        cols = ", ".join([self.target_key_col] + self.fields)
        placeholders = ", ".join("?" for _ in keys)
        query = (
            f"SELECT {cols} FROM {self.target_table} "
            f"WHERE {self.target_key_col} IN ({placeholders})"
        )

        rows = self.adapter.execute(query, tuple(keys))

        index = {row[self.target_key_col]: dict(row) for row in rows}

        for env in envelopes:
            key = self._extract_key(env)
            env.lookups[self.lookup_name] = index.get(key)

        return envelopes

    def enrich(self, envelope: Envelope) -> Envelope:
        """Single-message convenience wrapper, e.g. for a one-off test."""
        return self.enrich_batch([envelope])[0]

    def _extract_key(self, env: Envelope):
        """Resolve the canonical path to the lookup key value.

        Guards against unhashable types (dict, list) that would crash the
        downstream set/dict operations.
        """
        val = resolve_path(env.canonical_dict, self.source_key_field)
        if isinstance(val, (dict, list)):
            return None
        return val

