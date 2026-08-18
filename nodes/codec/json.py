"""JSON codec — the first concrete format ↔ canonical bridge.

Uses the canonical model itself as the JSON schema: ``parse`` validates
inbound JSON against the model; ``serialize`` emits a normalized (sorted-key)
JSON document so parsing it back yields an equal canonical message.

Error handling follows the taxonomy in ``core/errors``:
  * malformed / non-object JSON            -> ``DecodeError`` (stage decode)
  * valid JSON but not a valid model       -> ``CanonicalValidationError``
  * failure to serialize the model         -> ``SerializeError``
"""
from __future__ import annotations

import json

from pydantic import ValidationError as PydanticValidationError

from core.errors import CanonicalValidationError, DecodeError, SerializeError
from core.model import CanonicalMessage
from nodes.codec.base import Codec


class JsonCodec(Codec):
    key = "json"

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
                "JSON document must be an object to map onto the canonical model",
            )

        try:
            return CanonicalMessage.model_validate(data)
        except PydanticValidationError as e:
            raise CanonicalValidationError(
                "canonical.invalid",
                f"JSON does not satisfy the canonical model: {e}",
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