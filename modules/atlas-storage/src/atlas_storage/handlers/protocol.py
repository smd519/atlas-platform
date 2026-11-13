"""Protocol definition for pluggable storage format handlers."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from atlas_core import StorageProvider

from atlas_storage.models.storage_location import StorageLocation


@runtime_checkable
class FormatHandler(Protocol):
    """Structural contract for logical format serialization, loading, and existence checks."""

    def save(
        self,
        data: Any,
        location: StorageLocation,
        backend: StorageProvider,
    ) -> int | None:
        """Serialize ``data`` and persist it to ``location`` via ``backend``."""
        ...

    def load(
        self,
        location: StorageLocation,
        backend: StorageProvider,
    ) -> Any:
        """Load raw data from ``location`` via ``backend`` and deserialize it."""
        ...

    def exists(
        self,
        location: StorageLocation,
        backend: StorageProvider,
    ) -> bool:
        """Return True if the dataset at ``location`` logically exists in ``backend``."""
        ...


__all__ = ["FormatHandler"]

