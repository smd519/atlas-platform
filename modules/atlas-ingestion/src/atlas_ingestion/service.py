"""Public ingestion service and module entry point."""

from __future__ import annotations

from atlas_connectors.factory import ConnectorFactory
from atlas_connectors.registry import ConnectorRegistry
from atlas_core.contracts.storage import StorageProvider
from atlas_storage.manager import StorageManager
from atlas_storage.registry import StorageRegistry

from atlas_ingestion.extraction.extractor import Extractor
from atlas_ingestion.loading.loader import Loader
from atlas_ingestion.models.ingestion_request import IngestionRequest
from atlas_ingestion.models.ingestion_result import IngestionResult
from atlas_ingestion.pipeline import IngestionPipeline
from atlas_ingestion.tracking.execution_tracker import (
    ExecutionTracker,
    IngestionExecutionRecord,
)


class IngestionService:
    """Public facade for executing and inspecting data ingestion operations.

    Consumed by ``atlas-orchestration`` to run raw data acquisition without
    coupling orchestration workflows to connector or storage implementation details.
    """

    def __init__(
        self,
        pipeline: IngestionPipeline | None = None,
        *,
        extractor: Extractor | None = None,
        loader: Loader | None = None,
        tracker: ExecutionTracker | None = None,
        connector_factory: ConnectorFactory | None = None,
        connector_registry: ConnectorRegistry | None = None,
        storage_manager: StorageManager | None = None,
        storage_provider: StorageProvider | None = None,
        storage_registry: StorageRegistry | None = None,
    ) -> None:
        if pipeline is not None:
            self._pipeline = pipeline
        else:
            resolved_extractor = extractor or Extractor(
                connector_factory=connector_factory,
                connector_registry=connector_registry,
            )
            resolved_loader = loader or Loader(
                storage_manager=storage_manager,
                storage_provider=storage_provider,
                storage_registry=storage_registry,
            )
            resolved_tracker = tracker or ExecutionTracker()
            self._pipeline = IngestionPipeline(
                extractor=resolved_extractor,
                loader=resolved_loader,
                tracker=resolved_tracker,
            )

    def ingest(
        self,
        request: IngestionRequest,
        *,
        execution_id: str | None = None,
        raise_on_error: bool | None = None,
    ) -> IngestionResult:
        """Execute an ingestion request and return a standardized :class:`IngestionResult`."""
        return self._pipeline.run(
            request,
            execution_id=execution_id,
            raise_on_error=raise_on_error,
        )

    def get_execution(self, execution_id: str) -> IngestionExecutionRecord:
        """Retrieve the tracked :class:`IngestionExecutionRecord` for ``execution_id``."""
        return self._pipeline.tracker.get_execution(execution_id)

    def list_executions(self) -> list[IngestionExecutionRecord]:
        """List all tracked :class:`IngestionExecutionRecord` instances."""
        return self._pipeline.tracker.list_executions()


__all__ = [
    "IngestionService",
]
