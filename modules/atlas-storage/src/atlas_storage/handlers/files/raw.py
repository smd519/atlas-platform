"""Raw binary bytes format handler implementation."""

from __future__ import annotations

from typing import Any

from atlas_core import StorageProvider

from atlas_storage.exceptions import StorageReadError, StorageWriteError
from atlas_storage.models.storage_location import StorageLocation


class RawBytesHandler:
    """Format handler for passthrough raw binary bytes persistence."""

    def serialize(self, data: Any) -> bytes:
        """Convert ``data`` (``bytes``, ``bytearray``, or ``str``) into ``bytes``."""
        if data is None:
            raise StorageWriteError("Cannot serialize None payload as raw bytes.")
        if isinstance(data, (bytes, bytearray)):
            return bytes(data)
        if isinstance(data, str):
            return data.encode("utf-8")
        raise StorageWriteError(
            f"Raw byte storage format requires bytes or str, got '{type(data).__name__}'."
        )

    def deserialize(self, payload: Any) -> bytes:
        """Validate and return raw ``bytes`` from ``payload``."""
        if not isinstance(payload, (bytes, bytearray)):
            raise StorageReadError(
                f"Expected bytes payload from storage backend, got '{type(payload).__name__}'."
            )
        return bytes(payload)

    def save(
        self,
        data: Any,
        location: StorageLocation,
        backend: StorageProvider,
    ) -> int | None:
        """Persist raw bytes to ``location.path`` via ``backend``."""
        serialized = self.serialize(data)
        row_count = 1 if len(serialized) > 0 else 0
        backend.save(data=serialized, path=location.path)
        return row_count

    def load(
        self,
        location: StorageLocation,
        backend: StorageProvider,
    ) -> bytes:
        """Load raw bytes from ``location.path`` via ``backend``."""
        raw_payload = backend.load(path=location.path)
        return self.deserialize(raw_payload)

    def exists(
        self,
        location: StorageLocation,
        backend: StorageProvider,
    ) -> bool:
        """Return True if ``location.path`` exists in ``backend``."""
        return backend.exists(path=location.path)


__all__ = ["RawBytesHandler"]

