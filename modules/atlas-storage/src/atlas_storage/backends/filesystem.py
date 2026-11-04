from pathlib import Path
from typing import Any

from atlas_core.contracts.storage import StorageProvider


class FileSystemBackend(StorageProvider):

    def save(
        self,
        data: Any,
        path: str,
    ) -> None:
        path_obj = Path(path)

        path_obj.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        with open(path_obj, "wb") as file:
            file.write(data)

    def load(
        self,
        path: str,
    ) -> bytes:
        with open(path, "rb") as file:
            return file.read()

    def exists(
        self,
        path: str,
    ) -> bool:
        return Path(path).exists()