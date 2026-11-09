"""Registry for storage format handlers."""

from __future__ import annotations

from atlas_storage.exceptions import StorageConfigurationError
from atlas_storage.handlers.files.csv import CsvHandler
from atlas_storage.handlers.files.json import JsonHandler
from atlas_storage.handlers.files.parquet import ParquetHandler
from atlas_storage.handlers.files.raw import RawBytesHandler
from atlas_storage.handlers.protocol import FormatHandler

_REQUIRED_HANDLER_METHODS = ("save", "load", "exists")


def _normalize_format_name(name: str) -> str:
    if not isinstance(name, str):
        raise StorageConfigurationError(
            f"Format name must be a non-empty string, got '{type(name).__name__}'."
        )
    normalized = name.strip().lower()
    if not normalized:
        raise StorageConfigurationError("Format registration name cannot be empty.")
    return normalized


def _validate_handler_class(name: str, handler_class: type[FormatHandler]) -> None:
    if not isinstance(handler_class, type):
        raise StorageConfigurationError(
            f"Registered format handler for '{name}' must be a class, "
            f"got '{type(handler_class).__name__}'."
        )
    missing_methods = [
        method
        for method in _REQUIRED_HANDLER_METHODS
        if not callable(getattr(handler_class, method, None))
    ]
    if missing_methods:
        missing_str = ", ".join(missing_methods)
        raise StorageConfigurationError(
            f"Format handler class '{handler_class.__name__}' for '{name}' "
            f"is missing required FormatHandler methods: {missing_str}."
        )


class FormatRegistry:
    """Registers and instantiates :class:`FormatHandler` implementations by format name."""

    def __init__(self) -> None:
        self._handlers: dict[str, type[FormatHandler]] = {}

    def register(
        self,
        name: str,
        handler_class: type[FormatHandler],
    ) -> None:
        """Register ``handler_class`` under normalized format ``name``."""
        key = _normalize_format_name(name)
        _validate_handler_class(key, handler_class)
        if key in self._handlers:
            raise StorageConfigurationError(
                f"Storage format '{key}' is already registered with "
                f"'{self._handlers[key].__name__}'."
            )
        self._handlers[key] = handler_class

    def get(
        self,
        name: str,
    ) -> FormatHandler:
        """Instantiate and return a fresh :class:`FormatHandler` for ``name``."""
        key = _normalize_format_name(name)
        try:
            handler_class = self._handlers[key]
        except KeyError as exc:
            supported = ", ".join(sorted(self._handlers)) or "none"
            raise StorageConfigurationError(
                f"Unsupported storage format '{key}'. Registered formats: {supported}."
            ) from exc
        return handler_class()

    def exists(
        self,
        name: str,
    ) -> bool:
        """Return True if ``name`` is registered."""
        if not isinstance(name, str):
            return False
        key = name.strip().lower()
        if not key:
            return False
        return key in self._handlers

    def list_formats(self) -> list[str]:
        """Return the registered format identifiers."""
        return list(self._handlers.keys())


def create_default_format_registry() -> FormatRegistry:
    """Create a :class:`FormatRegistry` pre-populated with built-in format handlers."""
    registry = FormatRegistry()
    registry.register("csv", CsvHandler)
    registry.register("json", JsonHandler)
    registry.register("parquet", ParquetHandler)
    registry.register("raw", RawBytesHandler)
    registry.register("bytes", RawBytesHandler)
    return registry


__all__ = [
    "FormatRegistry",
    "create_default_format_registry",
]

