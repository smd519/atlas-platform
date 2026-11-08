"""Platform service initialization and dependency wiring for atlas-runner."""

from __future__ import annotations

from atlas_connectors import ConnectorRegistry, LocalFileConnector, RestConnector
from atlas_core import StorageProvider
from atlas_ingestion import IngestionService
from atlas_orchestration import OrchestrationDependencies, OrchestrationService
from atlas_processing import ProcessingService
from atlas_quality import QualityService
from atlas_storage import FileSystemBackend, StorageManager


def build_default_dependencies(
    *,
    storage_backend: StorageProvider | None = None,
    storage_manager: StorageManager | None = None,
    connector_registry: ConnectorRegistry | None = None,
    ingestion_service: IngestionService | None = None,
    processing_service: ProcessingService | None = None,
    quality_service: QualityService | None = None,
) -> OrchestrationDependencies:
    """Initialize platform services and return an :class:`OrchestrationDependencies` container."""
    backend = storage_backend or FileSystemBackend()
    resolved_storage = storage_manager or StorageManager(backend=backend)

    resolved_connectors = connector_registry or ConnectorRegistry()
    if not resolved_connectors.exists("local_file"):
        resolved_connectors.register("local_file", LocalFileConnector)
    if not resolved_connectors.exists("rest"):
        resolved_connectors.register("rest", RestConnector)

    resolved_ingestion = ingestion_service or IngestionService(
        connector_registry=resolved_connectors,
        storage_manager=resolved_storage,
    )
    resolved_processing = processing_service or ProcessingService(
        storage_manager=resolved_storage,
    )
    resolved_quality = quality_service or QualityService(
        storage_manager=resolved_storage,
    )

    return OrchestrationDependencies(
        ingestion_service=resolved_ingestion,
        processing_service=resolved_processing,
        quality_service=resolved_quality,
        storage_provider=backend,
    )


def create_orchestration_service(
    dependencies: OrchestrationDependencies | None = None,
) -> OrchestrationService:
    """Create an :class:`OrchestrationService` wired with platform dependencies."""
    resolved_deps = dependencies or build_default_dependencies()
    return OrchestrationService(dependencies=resolved_deps)


__all__ = [
    "build_default_dependencies",
    "create_orchestration_service",
]
