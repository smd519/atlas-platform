"""
Atlas Platform exception hierarchy.
"""


class AtlasError(Exception):
    """Base exception for all Atlas errors."""


class ConfigurationError(AtlasError):
    """Invalid or missing configuration."""


class ValidationError(AtlasError):
    """Data validation failure."""


class ConnectorError(AtlasError):
    """External source interaction failure."""


class IngestionError(AtlasError):
    """Data ingestion failure."""


class ProcessingError(AtlasError):
    """Data processing failure."""


class StorageError(AtlasError):
    """Data persistence failure."""


class MetadataError(AtlasError):
    """Metadata operation failure."""


class QualityError(AtlasError):
    """Data quality rule failure."""


class OrchestrationError(AtlasError):
    """Workflow execution failure."""
