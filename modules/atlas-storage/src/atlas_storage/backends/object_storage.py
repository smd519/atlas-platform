from typing import Any

from atlas_core.contracts.storage import StorageProvider


class ObjectStorageBackend(StorageProvider):
    """
    Base implementation placeholder for cloud object storage.

    Future implementations may support:
    - Amazon S3
    - Azure Blob Storage
    - Google Cloud Storage
    - MinIO
    """

    def save(
        self,
        data: Any,
        path: str,
    ) -> None:
        raise NotImplementedError(
            "Object storage backend is not implemented."
        )

    def load(
        self,
        path: str,
    ) -> Any:
        raise NotImplementedError(
            "Object storage backend is not implemented."
        )

    def exists(
        self,
        path: str,
    ) -> bool:
        raise NotImplementedError(
            "Object storage backend is not implemented."
        )