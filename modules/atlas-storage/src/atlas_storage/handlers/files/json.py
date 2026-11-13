"""JSON format handler implementation."""

from __future__ import annotations

import io
import json
from collections.abc import Sized
from typing import Any

import pandas as pd  # type: ignore[import-untyped]
from atlas_core import StorageProvider

from atlas_storage.exceptions import StorageReadError, StorageWriteError
from atlas_storage.models.storage_location import StorageLocation


def _compute_row_count(data: Any) -> int | None:
    if data is None:
        return None
    if isinstance(data, (bytes, bytearray, str)):
        return 1 if len(data) > 0 else 0
    if isinstance(data, Sized):
        return len(data)
    return None


class JsonHandler:
    """Format handler for UTF-8 JSON serialization and deserialization."""

    def serialize(self, data: Any) -> bytes:
        """Serialize ``data`` into UTF-8 encoded JSON bytes."""
        if data is None:
            raise StorageWriteError("Cannot serialize None payload as JSON.")
        if isinstance(data, (bytes, bytearray)):
            return bytes(data)
        if isinstance(data, str):
            return data.encode("utf-8")
        try:
            if isinstance(data, pd.DataFrame):
                json_str = data.to_json(orient="records")
                return (json_str or "[]").encode("utf-8")
            if isinstance(data, (list, dict)):
                return json.dumps(data).encode("utf-8")
            raise TypeError(f"Unsupported type '{type(data).__name__}' for JSON serialization.")
        except StorageWriteError:
            raise
        except Exception as exc:
            raise StorageWriteError(f"Failed to serialize data to JSON: {exc}") from exc

    def deserialize(self, payload: bytes) -> pd.DataFrame:
        """Deserialize JSON ``payload`` bytes into a :class:`pandas.DataFrame`."""
        if not isinstance(payload, (bytes, bytearray)):
            raise StorageReadError(
                f"Expected bytes payload for JSON deserialization, got '{type(payload).__name__}'."
            )
        raw = bytes(payload)
        try:
            parsed = json.loads(raw.decode("utf-8"))
            if isinstance(parsed, dict):
                return pd.DataFrame([parsed])
            if isinstance(parsed, list):
                return pd.DataFrame(parsed)
            return pd.read_json(io.BytesIO(raw))
        except Exception as exc:
            raise StorageReadError(f"Failed to deserialize JSON bytes: {exc}") from exc

    def save(
        self,
        data: Any,
        location: StorageLocation,
        backend: StorageProvider,
    ) -> int | None:
        """Serialize ``data`` as JSON and persist to ``location.path`` via ``backend``."""
        serialized = self.serialize(data)
        row_count = _compute_row_count(data)
        backend.save(data=serialized, path=location.path)
        return row_count

    def load(
        self,
        location: StorageLocation,
        backend: StorageProvider,
    ) -> pd.DataFrame:
        """Load raw bytes from ``location.path`` via ``backend`` and parse as JSON."""
        raw_payload = backend.load(path=location.path)
        return self.deserialize(raw_payload)

    def exists(
        self,
        location: StorageLocation,
        backend: StorageProvider,
    ) -> bool:
        """Return True if ``location.path`` exists in ``backend``."""
        return backend.exists(path=location.path)


__all__ = ["JsonHandler"]

