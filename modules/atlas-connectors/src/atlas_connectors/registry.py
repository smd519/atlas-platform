"""Registry for available data source connectors."""

from __future__ import annotations

from atlas_core import Connector

from atlas_connectors.exceptions import ConnectorConfigurationError


class ConnectorRegistry:
    """Registers and resolves :class:`Connector` implementations by type name."""

    def __init__(self) -> None:
        self._connectors: dict[str, type[Connector]] = {}

    def register(
        self,
        name: str,
        connector_class: type[Connector],
    ) -> None:
        """Register ``connector_class`` under ``name``."""
        key = name.strip() if isinstance(name, str) else ""
        if not key:
            raise ConnectorConfigurationError("Connector registration name cannot be empty.")
        if not isinstance(connector_class, type) or not issubclass(connector_class, Connector):
            raise ConnectorConfigurationError(
                f"Registered class for '{key}' must be a subclass of Connector."
            )
        self._connectors[key] = connector_class

    def get(
        self,
        name: str,
    ) -> type[Connector]:
        """Return the :class:`Connector` subclass registered under ``name``."""
        try:
            return self._connectors[name]
        except KeyError as exc:
            raise ConnectorConfigurationError(
                f"Connector type '{name}' is not registered."
            ) from exc

    def exists(
        self,
        name: str,
    ) -> bool:
        """Return True if ``name`` is registered."""
        return name in self._connectors

    def list_connectors(self) -> list[str]:
        """Return the registered connector type names."""
        return list(self._connectors.keys())


__all__ = ["ConnectorRegistry"]
