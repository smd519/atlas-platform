from atlas_core.exceptions import AtlasError


class StorageError(AtlasError):
    """Base storage exception."""


class StorageConfigurationError(StorageError):
    """Invalid storage configuration."""


class StorageBackendError(StorageError):
    """Storage backend operation failed."""


class StorageReadError(StorageError):
    """Failed to read data."""


class StorageWriteError(StorageError):
    """Failed to write data."""


class SerializationError(StorageError):
    """Failed to serialize data."""


class PartitioningError(StorageError):
    """Partition generation failed."""