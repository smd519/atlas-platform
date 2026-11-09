"""CSV format handler implementation."""

from __future__ import annotations

import io
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


def _coerce_dataframe(data: Any) -> pd.DataFrame:
    if isinstance(data, pd.DataFrame):
        return data
    if isinstance(data, list):
        return pd.DataFrame(data)
    if isinstance(data, dict):
        return pd.DataFrame([data])
    raise TypeError(f"Cannot serialize object of type '{type(data).__name__}' as CSV.")


class CsvHandler:
    """Format handler for UTF-8 CSV serialization and deserialization."""

    def serialize(self, data: Any) -> bytes:
        """Serialize ``data`` into UTF-8 encoded CSV bytes."""
        if data is None:
            raise StorageWriteError("Cannot serialize None payload as CSV.")
        if isinstance(data, (bytes, bytearray)):
            return bytes(data)
        if isinstance(data, str):
            return data.encode("utf-8")
        try:
            dataframe = _coerce_dataframe(data)
            csv_text = dataframe.to_csv(index=False)
            return (csv_text or "").encode("utf-8")
        except StorageWriteError:
            raise
        except Exception as exc:
            raise StorageWriteError(f"Failed to serialize data to CSV: {exc}") from exc

    def deserialize(self, payload: bytes) -> pd.DataFrame:
        """Deserialize CSV ``payload`` bytes into a :class:`pandas.DataFrame`."""
        if not isinstance(payload, (bytes, bytearray)):
            raise StorageReadError(
                f"Expected bytes payload for CSV deserialization, got '{type(payload).__name__}'."
            )
        try:
            return pd.read_csv(io.BytesIO(bytes(payload)))
        except Exception as exc:
            raise StorageReadError(f"Failed to deserialize CSV bytes: {exc}") from exc

    def save(
        self,
        data: Any,
        location: StorageLocation,
        backend: StorageProvider,
    ) -> int | None:
        """Serialize ``data`` as CSV and persist to ``location.path`` via ``backend``."""
        serialized = self.serialize(data)
        row_count = _compute_row_count(data)
        backend.save(data=serialized, path=location.path)
        return row_count

    def load(
        self,
        location: StorageLocation,
        backend: StorageProvider,
    ) -> pd.DataFrame:
        """Load raw bytes from ``location.path`` via ``backend`` and parse as CSV."""
        raw_payload = backend.load(path=location.path)
        return self.deserialize(raw_payload)

    def exists(
        self,
        location: StorageLocation,
        backend: StorageProvider,
    ) -> bool:
        """Return True if ``location.path`` exists in ``backend``."""
        return backend.exists(path=location.path)


__all__ = ["CsvHandler"]
