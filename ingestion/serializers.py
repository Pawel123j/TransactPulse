"""Pluggable record serializers.

JSON is the chosen wire format (ADR 0003). The :class:`Serializer` protocol keeps
an Avro upgrade path open without changing the generator or producer call sites.
"""

from __future__ import annotations

import json
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Serializer(Protocol):
    """Serialize a record dict to bytes for the wire."""

    content_type: str

    def serialize(self, record: dict[str, Any]) -> bytes:
        """Return the encoded representation of ``record``."""
        ...


class JsonSerializer:
    """UTF-8 JSON serializer (compact, human-readable in Kafka UI)."""

    content_type = "application/json"

    def __init__(self, *, sort_keys: bool = False) -> None:
        self._sort_keys = sort_keys

    def serialize(self, record: dict[str, Any]) -> bytes:
        """Encode ``record`` as compact UTF-8 JSON bytes."""
        return json.dumps(
            record,
            separators=(",", ":"),
            ensure_ascii=False,
            sort_keys=self._sort_keys,
        ).encode("utf-8")


# NOTE: An AvroSerializer (Schema-Registry backed) can be added here implementing
# the same Serializer protocol if/when ADR 0003 is revisited.
