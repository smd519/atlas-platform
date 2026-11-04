"""Public processing service and module entry point."""

from __future__ import annotations

from atlas_core import StorageProvider
from atlas_storage import StorageManager, StorageRegistry

from atlas_processing.engine import ProcessingEngine
from atlas_processing.execution.executor import TransformationExecutor
from atlas_processing.models.processing_request import ProcessingRequest
from atlas_processing.models.processing_result import ProcessingResult
from atlas_processing.pipeline import ProcessingPipeline
from atlas_processing.tracking.execution_tracker import (
    ExecutionTracker,
    ProcessingExecutionRecord,
)
from atlas_processing.transformations.registry import TransformationRegistry


class ProcessingService:
    """Public facade for executing and inspecting dataset transformation operations.

    Consumed by ``atlas-orchestration`` to run configurable dataset transformations
    from raw/source storage locations into curated (`silver` / `gold`) storage layers.
    """

    def __init__(
        self,
        pipeline: ProcessingPipeline | None = None,
        *,
        executor: TransformationExecutor | None = None,
        engine: ProcessingEngine | None = None,
        transformation_registry: TransformationRegistry | None = None,
        tracker: ExecutionTracker | None = None,
        storage_manager: StorageManager | None = None,
        storage_provider: StorageProvider | None = None,
        storage_registry: StorageRegistry | None = None,
    ) -> None:
        if pipeline is not None:
            self._pipeline = pipeline
        else:
            resolved_executor = executor or TransformationExecutor(
                engine=engine,
                registry=transformation_registry,
            )
            self._pipeline = ProcessingPipeline(
                executor=resolved_executor,
                storage_manager=storage_manager,
                tracker=tracker,
                storage_provider=storage_provider,
                storage_registry=storage_registry,
            )

    def process(
        self,
        request: ProcessingRequest,
        *,
        execution_id: str | None = None,
        raise_on_error: bool | None = None,
    ) -> ProcessingResult:
        """Execute a processing request and return a standardized :class:`ProcessingResult`."""
        return self._pipeline.run(
            request,
            execution_id=execution_id,
            raise_on_error=raise_on_error,
        )

    def get_execution(self, execution_id: str) -> ProcessingExecutionRecord:
        """Retrieve the tracked :class:`ProcessingExecutionRecord` for ``execution_id``."""
        return self._pipeline.tracker.get_execution(execution_id)

    def list_executions(self) -> list[ProcessingExecutionRecord]:
        """List all tracked :class:`ProcessingExecutionRecord` instances."""
        return self._pipeline.tracker.list_executions()


__all__ = [
    "ProcessingService",
]

