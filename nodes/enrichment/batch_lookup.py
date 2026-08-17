import sqlite3
from typing import List

from core.message import Envelope
from nodes.base import EnrichmentNode


class BatchLookup(EnrichmentNode):
    """Case 2 from the blueprint: resolves reference data (patient name,
    doctor id, test code...) for a whole batch of envelopes in one query,
    instead of one query per message (the N+1 pattern that kills Mirth
    channels under load).

    db_path defaults to the same reference DB the source system uses — point
    it at whatever table holds the lookup data, NOT necessarily queue.db.
    """

    def __init__(self, db_path: str, source_key_field: str, target_table: str,
                 target_key_col: str, fields: List[str], lookup_name: str):
        self.db_path = db_path
        self.source_key_field = source_key_field  # field name inside raw_payload
        self.target_table = target_table
        self.target_key_col = target_key_col
        self.fields = fields
        self.lookup_name = lookup_name

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def enrich_batch(self, envelopes: List[Envelope]) -> List[Envelope]:
        keys = {self._extract_key(e) for e in envelopes}
        keys.discard(None)
        if not keys:
            for env in envelopes:
                env.lookups[self.lookup_name] = None
            return envelopes

        cols = ", ".join([self.target_key_col] + self.fields)
        placeholders = ", ".join("?" for _ in keys)
        query = f"SELECT {cols} FROM {self.target_table} WHERE {self.target_key_col} IN ({placeholders})"

        with self._get_conn() as conn:
            rows = conn.execute(query, tuple(keys)).fetchall()

        index = {row[self.target_key_col]: dict(row) for row in rows}

        for env in envelopes:
            key = self._extract_key(env)
            # missing lookup is attached as None, not silently dropped —
            # a required field in the mapping downstream will catch it and DLQ the message
            env.lookups[self.lookup_name] = index.get(key)

        return envelopes

    def enrich(self, envelope: Envelope) -> Envelope:
        """Single-message convenience wrapper, e.g. for a one-off test."""
        return self.enrich_batch([envelope])[0]

    def _extract_key(self, env: Envelope):
        return env.raw_payload.get(self.source_key_field) if isinstance(env.raw_payload, dict) else None
