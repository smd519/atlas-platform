"""Unified storage manager coordinating format handlers and backend persistence."""

from __future__ import annotations

from typing import Any

from atlas_core import StorageProvider

from atlas_storage.exceptions import (
    StorageConfigurationError,
    StorageError,
    StorageReadError,
    StorageWriteError,
)
from atlas_storage.handlers.registry import FormatRegistry, create_default_format_registry
from atlas_storage.models.storage_location import StorageLocation
from atlas_storage.models.storage_result import StorageResult


class StorageManager:
    """Format-agnostic coordinator delegating format handling to :class:`FormatRegistry`.

    Architecture:
    ``StorageManager -> FormatRegistry -> FormatHandler -> StorageProvider``
    """

    def __init__(
        self,
        backend: StorageProvider,
        format_registry: FormatRegistry | None = None,
    ) -> None:
        if not isinstance(backend, StorageProvider):
            raise StorageConfigurationError(
                "StorageManager requires a StorageProvider instance, "
                f"got '{type(backend).__name__}'."
            )
        if format_registry is not None and not isinstance(format_registry, FormatRegistry):
            raise StorageConfigurationError(
                "StorageManager requires a FormatRegistry instance when provided, "
                f"got '{type(format_registry).__name__}'."
            )
        self._backend = backend
        self._format_registry = (
            format_registry if format_registry is not None else create_default_format_registry()
        )

    @property
    def backend(self) -> StorageProvider:
        """Return the configured :class:`StorageProvider` backend."""
        return self._backend

    @property
    def format_registry(self) -> FormatRegistry:
        """Return the configured :class:`FormatRegistry`."""
        return self._format_registry

    def save(
        self,
        data: Any,
        location: StorageLocation,
    ) -> StorageResult:
        """Resolve a format handler for ``location.format`` and persist ``data``."""
        self._validate_location(location)
        handler = self._format_registry.get(location.format)

        try:
            row_count = handler.save(data, location, self._backend)
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
        """Resolve a format handler for ``location.format`` and load dataset from ``location``."""
        self._validate_location(location)
        handler = self._format_registry.get(location.format)

        try:
            return handler.load(location, self._backend)
        except StorageError:
            raise
        except Exception as exc:
            raise StorageReadError(f"Failed to read data from '{location.path}': {exc}") from exc

    def exists(
        self,
        location: StorageLocation,
    ) -> bool:
        """Resolve a format handler for ``location.format`` and check logical existence."""
        self._validate_location(location)
        handler = self._format_registry.get(location.format)

        try:
            return handler.exists(location, self._backend)
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


__all__ = ["StorageManager"]
