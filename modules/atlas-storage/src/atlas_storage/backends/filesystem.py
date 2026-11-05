"""Local filesystem storage backend."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from atlas_core import StorageProvider

from atlas_storage.exceptions import (
    StorageConfigurationError,
    StorageReadError,
    StorageWriteError,
)


class FileSystemBackend(StorageProvider):
    """Physical storage backend that reads and writes bytes on the local filesystem."""

    def save(
        self,
        data: Any,
        path: str,
    ) -> None:
        """Write raw ``data`` bytes to ``path``."""
        cleaned_path = path.strip() if isinstance(path, str) else ""
        if not cleaned_path:
            raise StorageConfigurationError("FileSystemBackend requires a non-empty path.")
        if not isinstance(data, (bytes, bytearray)):
            raise StorageWriteError(
                f"FileSystemBackend expects serialized bytes, got '{type(data).__name__}'."
            )

        path_obj = Path(cleaned_path)
        try:
            path_obj.parent.mkdir(
                parents=True,
                exist_ok=True,
            )
            with open(path_obj, "wb") as file:
                file.write(bytes(data))
        except Exception as exc:
            raise StorageWriteError(
                f"Failed to write bytes to filesystem path '{cleaned_path}': {exc}"
            ) from exc

    def load(
        self,
        path: str,
    ) -> bytes:
        """Read raw bytes from ``path``."""
        cleaned_path = path.strip() if isinstance(path, str) else ""
        if not cleaned_path:
            raise StorageConfigurationError("FileSystemBackend requires a non-empty path.")

        path_obj = Path(cleaned_path)
        if not path_obj.exists():
            raise StorageReadError(f"Storage file does not exist: '{cleaned_path}'.")
        try:
            with open(path_obj, "rb") as file:
                return file.read()
        except Exception as exc:
            raise StorageReadError(
                f"Failed to read bytes from filesystem path '{cleaned_path}': {exc}"
            ) from exc

    def exists(
        self,
        path: str,
    ) -> bool:
        """Return True if ``path`` exists on the local filesystem."""
        cleaned_path = path.strip() if isinstance(path, str) else ""
        if not cleaned_path:
            return False
        return Path(cleaned_path).exists()


__all__ = ["FileSystemBackend"]
