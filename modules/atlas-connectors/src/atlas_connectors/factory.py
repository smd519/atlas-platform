"""Factory for instantiating registered connectors."""

from __future__ import annotations

from typing import Any

from atlas_core import Connector

from atlas_connectors.exceptions import ConnectorConfigurationError, ConnectorError
from atlas_connectors.models.source_config import SourceConfig
from atlas_connectors.registry import ConnectorRegistry


class ConnectorFactory:
    """Creates :class:`Connector` instances from registered connector types."""

    def __init__(self, registry: ConnectorRegistry) -> None:
        self._registry = registry

    @property
    def registry(self) -> ConnectorRegistry:
        """Return the underlying :class:`ConnectorRegistry`."""
        return self._registry

    def create(
        self,
        connector_type: str,
        *args: Any,
        **kwargs: Any,
    ) -> Connector:
        """Instantiate the connector registered under ``connector_type``."""
        connector_class = self._registry.get(connector_type)
        try:
            return connector_class(*args, **kwargs)
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorConfigurationError(
                f"Failed to initialize connector '{connector_type}': {exc}"
            ) from exc

    def from_source_config(self, source_config: SourceConfig) -> Connector:
        """Instantiate a :class:`Connector` from a :class:`SourceConfig` model."""
        return self.create(source_config.source_type, **source_config.config)


__all__ = ["ConnectorFactory"]
