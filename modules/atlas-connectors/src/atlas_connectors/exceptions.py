from atlas_core.exceptions import AtlasError


class ConnectorError(AtlasError):
    """Base connector exception."""


class ConnectorConfigurationError(ConnectorError):
    """Connector configuration is invalid."""


class ConnectorConnectionError(ConnectorError):
    """Unable to connect to source."""


class ConnectorAuthenticationError(ConnectorError):
    """Authentication failed."""


class ConnectorExtractionError(ConnectorError):
    """Data extraction failed."""


class ParserError(ConnectorError):
    """Payload parsing failed."""