"""Public platform contracts for atlas-core."""

from atlas_core.contracts.connector import Connector
from atlas_core.contracts.execution import ExecutionManager
from atlas_core.contracts.metadata import MetadataProvider
from atlas_core.contracts.processor import Processor
from atlas_core.contracts.quality import QualityRule
from atlas_core.contracts.storage import StorageProvider

__all__ = [
    "Connector",
    "ExecutionManager",
    "MetadataProvider",
    "Processor",
    "QualityRule",
    "StorageProvider",
]
