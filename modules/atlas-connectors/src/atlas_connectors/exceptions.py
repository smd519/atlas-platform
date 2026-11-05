"""Connector-specific exception hierarchy."""

from __future__ import annotations

from atlas_core import ConnectorError as CoreConnectorError


class ConnectorError(CoreConnectorError):
    """Base connector exception."""


class ConnectorConfigurationError(ConnectorError):
    """Connector configuration is invalid or unregistered."""


class ConnectorConnectionError(ConnectorError):
    """Unable to connect to source."""


class ConnectorAuthenticationError(ConnectorError):
    """Authentication failed."""


class ConnectorExtractionError(ConnectorError):
    """Data extraction failed."""


class ParserError(ConnectorError):
    """Payload parsing failed."""


__all__ = [
    "ConnectorAuthenticationError",
    "ConnectorConfigurationError",
    "ConnectorConnectionError",
    "ConnectorError",
    "ConnectorExtractionError",
    "ParserError",
]
