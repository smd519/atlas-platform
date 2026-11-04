"""Registry for storage backends."""

from __future__ import annotations

from atlas_core import StorageProvider

from atlas_storage.exceptions import StorageConfigurationError


class StorageRegistry:
    """Registers and resolves :class:`StorageProvider` backend implementations."""

    def __init__(self) -> None:
        self._backends: dict[str, type[StorageProvider]] = {}

    def register(
        self,
        name: str,
        backend_class: type[StorageProvider],
    ) -> None:
        """Register ``backend_class`` under ``name``."""
        key = name.strip() if isinstance(name, str) else ""
        if not key:
            raise StorageConfigurationError("Storage backend registration name cannot be empty.")
        if not isinstance(backend_class, type) or not issubclass(backend_class, StorageProvider):
            raise StorageConfigurationError(
                f"Registered backend for '{key}' must be a subclass of StorageProvider."
            )
        self._backends[key] = backend_class

    def get(
        self,
        name: str,
    ) -> type[StorageProvider]:
        """Return the :class:`StorageProvider` class registered under ``name``."""
        try:
            return self._backends[name]
        except KeyError as exc:
            raise StorageConfigurationError(f"Storage backend '{name}' is not registered.") from exc

    def exists(
        self,
        name: str,
    ) -> bool:
        """Return True if ``name`` is registered."""
        return name in self._backends

    def list_backends(self) -> list[str]:
        """Return the registered backend names."""
        return list(self._backends.keys())


__all__ = ["StorageRegistry"]
