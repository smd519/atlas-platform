"""Unified storage manager coordinating format serialization and backend persistence."""

from __future__ import annotations

from collections.abc import Sized
from typing import Any

from atlas_core import StorageProvider

from atlas_storage.exceptions import (
    StorageConfigurationError,
    StorageError,
    StorageReadError,
    StorageWriteError,
)
from atlas_storage.formats import deserialize_data, serialize_data
from atlas_storage.models.storage_location import StorageLocation
from atlas_storage.models.storage_result import StorageResult


class StorageManager:
    """Coordinates format serialization/deserialization and physical backend persistence.

    Architecture:
    ``StorageManager -> Format Serializer -> Serialized Bytes -> Storage Backend``
    """

    def __init__(
        self,
        backend: StorageProvider,
    ) -> None:
        if not isinstance(backend, StorageProvider):
            raise StorageConfigurationError(
                "StorageManager requires a StorageProvider instance, "
                f"got '{type(backend).__name__}'."
            )
        self._backend = backend

    @property
    def backend(self) -> StorageProvider:
        """Return the configured :class:`StorageProvider` backend."""
        return self._backend

    def save(
        self,
        data: Any,
        location: StorageLocation,
    ) -> StorageResult:
        """Serialize ``data`` according to ``location.format`` and persist via the backend."""
        self._validate_location(location)
        row_count = self._compute_row_count(data)
        serialized_bytes = serialize_data(data, location.format)

        try:
            self._backend.save(
                data=serialized_bytes,
                path=location.path,
            )
        except StorageError:
            raise
        except Exception as exc:
            raise StorageWriteError(f"Failed to persist data to '{location.path}': {exc}") from exc

        return StorageResult(
            success=True,
            path=location.path,
            row_count=row_count,
        )

    def load(
        self,
        location: StorageLocation,
    ) -> Any:
        """Load raw bytes from ``location.path`` and deserialize for ``location.format``."""
        self._validate_location(location)

        try:
            raw_payload = self._backend.load(
                path=location.path,
            )
        except StorageError:
            raise
        except Exception as exc:
            raise StorageReadError(f"Failed to read data from '{location.path}': {exc}") from exc

        if isinstance(raw_payload, (bytes, bytearray)):
            return deserialize_data(bytes(raw_payload), location.format)
        return raw_payload

    def exists(
        self,
        location: StorageLocation,
    ) -> bool:
        """Return True if ``location.path`` exists in the backend."""
        self._validate_location(location)
        try:
            return self._backend.exists(
                path=location.path,
            )
        except StorageError:
            raise
        except Exception as exc:
            raise StorageReadError(
                f"Failed to check existence of '{location.path}': {exc}"
            ) from exc

    @staticmethod
    def _validate_location(location: StorageLocation) -> None:
        if not isinstance(location, StorageLocation):
            raise StorageConfigurationError(
                f"Expected StorageLocation instance, got '{type(location).__name__}'."
            )

    @staticmethod
    def _compute_row_count(data: Any) -> int | None:
        if data is None:
            return None
        if isinstance(data, (bytes, bytearray, str)):
            return 1 if len(data) > 0 else 0
        if isinstance(data, Sized):
            return len(data)
        return None


__all__ = ["StorageManager"]
