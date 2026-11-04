"""Storage backends for atlas-storage."""

from atlas_storage.backends.filesystem import FileSystemBackend
from atlas_storage.backends.object_storage import ObjectStorageBackend

__all__ = [
    "FileSystemBackend",
    "ObjectStorageBackend",
]
