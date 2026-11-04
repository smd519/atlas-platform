"""Public package interface for atlas-connectors."""

from atlas_connectors.exceptions import (
    ConnectorAuthenticationError,
    ConnectorConfigurationError,
    ConnectorConnectionError,
    ConnectorError,
    ConnectorExtractionError,
    ParserError,
)
from atlas_connectors.factory import ConnectorFactory
from atlas_connectors.files import LocalFileConnector
from atlas_connectors.http import (
    ApiKeyAuth,
    AuthStrategy,
    BearerTokenAuth,
    RestConnector,
    RestConnectorAdapter,
)
from atlas_connectors.models import ExtractionResult, SourceConfig
from atlas_connectors.parsers import parse_csv, parse_json, parse_parquet
from atlas_connectors.registry import ConnectorRegistry

__all__ = [
    "ApiKeyAuth",
    "AuthStrategy",
    "BearerTokenAuth",
    "ConnectorAuthenticationError",
    "ConnectorConfigurationError",
    "ConnectorConnectionError",
    "ConnectorError",
    "ConnectorExtractionError",
    "ConnectorFactory",
    "ConnectorRegistry",
    "ExtractionResult",
    "LocalFileConnector",
    "ParserError",
    "RestConnector",
    "RestConnectorAdapter",
    "SourceConfig",
    "parse_csv",
    "parse_json",
    "parse_parquet",
]

