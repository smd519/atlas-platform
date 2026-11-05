"""Public package interface for atlas-core."""

from atlas_core.constants import BRONZE, GOLD, SILVER
from atlas_core.contracts import (
    Connector,
    ExecutionManager,
    MetadataProvider,
    Processor,
    QualityRule,
    StorageProvider,
)
from atlas_core.enums import ExecutionStatus
from atlas_core.exceptions import (
    AtlasError,
    ConfigurationError,
    ConnectorError,
    IngestionError,
    MetadataError,
    OrchestrationError,
    ProcessingError,
    QualityError,
    StorageError,
    ValidationError,
)
from atlas_core.identifiers import generate_dataset_id, generate_execution_id
from atlas_core.logging import configure_logging, get_logger
from atlas_core.models import (
    Artifact,
    Dataset,
    Execution,
    QualityResult,
    Result,
    Source,
)
from atlas_core.paths import ensure_directory
from atlas_core.settings import AtlasSettings, settings
from atlas_core.types import DatasetId, JobId, SourceId
from atlas_core.validation import require_not_empty

__all__ = [
    "Artifact",
    "AtlasError",
    "AtlasSettings",
    "BRONZE",
    "ConfigurationError",
    "Connector",
    "ConnectorError",
    "Dataset",
    "DatasetId",
    "Execution",
    "ExecutionManager",
    "ExecutionStatus",
    "GOLD",
    "IngestionError",
    "JobId",
    "MetadataError",
    "MetadataProvider",
    "OrchestrationError",
    "ProcessingError",
    "Processor",
    "QualityError",
    "QualityResult",
    "QualityRule",
    "Result",
    "SILVER",
    "Source",
    "SourceId",
    "StorageError",
    "StorageProvider",
    "ValidationError",
    "configure_logging",
    "ensure_directory",
    "generate_dataset_id",
    "generate_execution_id",
    "get_logger",
    "require_not_empty",
    "settings",
]
