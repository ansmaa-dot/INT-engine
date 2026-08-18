"""Explicit codec registry.

Codec selection is exact: an unknown or misspelled key raises — it NEVER
silently falls back to a passthrough/default codec. Passthrough exists only
as an explicitly-registered codec (see nodes/codec/passthrough.py), never as
an implicit fallback.
"""
from __future__ import annotations

from nodes.codec.base import Codec


class CodecNotFoundError(KeyError):
    """Raised when a codec key isn't registered."""

    def __init__(self, key: str):
        super().__init__(f"no codec registered for key: {key!r}")
        self.key = key


REGISTRY: dict[str, Codec] = {}


def register(codec: Codec) -> Codec:
    """Register a codec instance under ``codec.key``. Returns it."""
    REGISTRY[codec.key] = codec
    return codec


def get(key: str) -> Codec:
    """Return the codec registered under ``key``.

    Raises ``CodecNotFoundError`` for any unknown key — no implicit fallback.
    """
    try:
        return REGISTRY[key]
    except KeyError:
        raise CodecNotFoundError(key) from None


def keys() -> list[str]:
    """All currently registered codec keys, sorted."""
    return sorted(REGISTRY)