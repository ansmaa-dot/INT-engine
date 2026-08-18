"""Explicit passthrough codec for simple/legacy channels.

Only ever used when a channel *explicitly* requests the ``passthrough`` codec
key. It is never an implicit fallback for unknown keys — see
``nodes/codec/registry.get()``.

It preserves the original wire text verbatim in ``extensions`` so nothing is
lost, and ``serialize`` returns it unchanged.
"""
from __future__ import annotations

import json

from core.model import CanonicalMessage
from nodes.codec.base import Codec


class PassthroughCodec(Codec):
    key = "passthrough"

    _RAW_EXT = "_passthrough_raw"

    def parse(self, raw, metadata=None):
        text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw
        canonical = CanonicalMessage(extensions={self._RAW_EXT: text})
        if metadata is not None:
            canonical.metadata.format = metadata.content_type or "passthrough"
            canonical.metadata.source = metadata.source
            canonical.metadata.message_id = metadata.message_id
            canonical.metadata.received_at = metadata.received_at
        return canonical

    def serialize(self, canonical):
        raw = canonical.extensions.get(self._RAW_EXT)
        if isinstance(raw, str):
            return raw
        return json.dumps(
            canonical.model_dump(mode="json"),
            ensure_ascii=False,
            sort_keys=True,
        )