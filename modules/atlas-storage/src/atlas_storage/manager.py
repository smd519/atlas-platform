from atlas_core.contracts.storage import StorageProvider

from atlas_storage.models.storage_location import StorageLocation
from atlas_storage.models.storage_result import StorageResult


class StorageManager:
    """
    Coordinates storage operations through a configured backend.

    The manager does not contain backend-specific logic.
    All read/write operations are delegated to the selected
    StorageProvider implementation.
    """

    def __init__(
        self,
        backend: StorageProvider,
    ) -> None:
        self._backend = backend

    def save(
        self,
        data,
        location: StorageLocation,
    ) -> StorageResult:
        self._backend.save(
            data=data,
            path=location.path,
        )

        row_count = (
            len(data)
            if hasattr(data, "__len__")
            else None
        )

        return StorageResult(
            success=True,
            path=location.path,
            row_count=row_count,
        )

    def load(
        self,
        location: StorageLocation,
    ):
        return self._backend.load(
            path=location.path,
        )

    def exists(
        self,
        location: StorageLocation,
    ) -> bool:
        return self._backend.exists(
            path=location.path,
        )