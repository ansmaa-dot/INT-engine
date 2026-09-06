"""Schemaless / raw JSON codec for arbitrary inbound payloads.

The ``json`` codec is deliberately strict: it validates the document against
the canonical model. Flat or unknown keys are silently *ignored* by
``CanonicalMessage.model_validate`` (Pydantic ``extra='ignore'`` is the
default), so a db_poller/webhook payload like::

    {"patient.name": "John", "patient.identifiers.0.value": "123"}

would pass decode AND validation with ``patient=None`` — and outbound HL7 PID
would silently build from nothing.

``schemaless.json`` is the shape-normalization boundary for exactly those
sources. Its ``parse()`` pipeline:

  1. decode bytes -> JSON object                  (json.* error taxonomy)
  2. ``unflatten_dot_keys`` -> nested dicts/lists (core/canonical_paths)
  3. project the canonical top-level groups into the model; any OTHER inbound
     key is preserved under ``extensions.<path>`` so nothing is dropped
  4. ``CanonicalMessage.model_validate``          (same pydantic coercion)

Outbound ``serialize`` emits the normalized canonical JSON (identical to
``json``) so a schemaless channel still produces parseable canonical JSON.

This lives in the codec — not a pipeline step — because ``Codec.parse()`` is
the first phase of ``ChannelRunner`` and owns wire↔canonical transcoding.
"""
from __future__ import annotations

import json

from pydantic import ValidationError as PydanticValidationError

from core.canonical_paths import assign_path, unflatten_dot_keys
from core.errors import CanonicalValidationError, DecodeError, SerializeError
from core.model import CanonicalMessage
from nodes.codec.base import Codec

#: Canonical top-level keys that validate into the model directly. Anything
#: else in an inbound document is preserved under ``extensions.*``.
_TOP_LEVEL_GROUPS = frozenset(
    ("schema_version", "patient", "encounter", "order",
     "specimen", "observations", "metadata", "extensions")
)


class SchemalessJsonCodec(Codec):
    key = "schemaless.json"
    structure = "schemaless"

    def parse(self, raw, metadata=None):
        if isinstance(raw, bytes):
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError as e:
                raise DecodeError(
                    "json.undecodable", f"bytes are not valid UTF-8: {e}",
                    cause=e,
                ) from e
        else:
            text = raw

        try:
            data = json.loads(text)
        except (TypeError, ValueError) as e:
            raise DecodeError(
                "json.invalid", f"malformed JSON: {e}", cause=e,
            ) from e

        if not isinstance(data, dict):
            raise DecodeError(
                "json.not_object",
                "schemaless JSON document must be an object; arrays belong in "
                "the source config (record extraction) or a structured codec",
            )

        # Shape-normalization boundary: fold flat dotted keys into a nested
        # tree, then project the canonical groups; every other inbound key is
        # preserved under extensions so arbitrary payloads are never dropped.
        data = unflatten_dot_keys(data)
        groups: dict = {}
        loose: dict = {}
        for key, value in data.items():
            if key in _TOP_LEVEL_GROUPS:
                groups[key] = value
            else:
                loose[key] = value
        if loose:
            existing = groups.get("extensions")
            merged = dict(existing) if isinstance(existing, dict) else {}
            for key, value in loose.items():
                assign_path(merged, key, value)
            groups["extensions"] = merged

        # Populate provenance from transport context when the payload carries
        # no metadata block of its own (mirrors PassthroughCodec).
        if metadata is not None and groups.get("metadata") is None:
            groups["metadata"] = {
                "format": self.key,
                "source": metadata.source,
                "message_id": metadata.message_id,
                "received_at": (
                    metadata.received_at.isoformat()
                    if metadata.received_at is not None else None
                ),
            }

        try:
            return CanonicalMessage.model_validate(groups)
        except PydanticValidationError as e:
            raise CanonicalValidationError(
                "canonical.invalid",
                f"schemaless JSON does not satisfy the canonical model: {e}",
                cause=e,
            ) from e

    def serialize(self, canonical):
        try:
            return json.dumps(
                canonical.model_dump(mode="json"),
                ensure_ascii=False,
                sort_keys=True,
            )
        except (TypeError, ValueError) as e:
            raise SerializeError(
                "json.serialize",
                f"could not serialize canonical message: {e}",
                cause=e,
            ) from e