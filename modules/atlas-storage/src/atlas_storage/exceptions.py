"""Storage-specific exception hierarchy."""

from __future__ import annotations

from atlas_core import StorageError as CoreStorageError


class StorageError(CoreStorageError):
    """Base storage exception."""


class StorageConfigurationError(StorageError):
    """Invalid storage configuration or location."""


class StorageBackendError(StorageError):
    """Storage backend operation failed."""


class StorageReadError(StorageBackendError):
    """Failed to read data from storage backend."""


class StorageWriteError(StorageBackendError):
    """Failed to write data to storage backend."""


class SerializationError(StorageError):
    """Failed to serialize or deserialize storage payload."""


class PartitioningError(StorageError):
    """Partition path generation failed."""


__all__ = [
    "PartitioningError",
    "SerializationError",
    "StorageBackendError",
    "StorageConfigurationError",
    "StorageError",
    "StorageReadError",
    "StorageWriteError",
]
