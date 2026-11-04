"""Coordinates dataset reading, transformation execution, curated persistence, and tracking."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import PurePath
from typing import Any

from atlas_core import (
    Artifact,
    ExecutionStatus,
    StorageProvider,
    generate_execution_id,
    get_logger,
)
from atlas_storage import (
    FileSystemBackend,
    PartitionStrategy,
    StorageError,
    StorageLocation,
    StorageManager,
    StorageRegistry,
)

from atlas_processing.exceptions import (
    InvalidProcessingRequestError,
    ProcessingError,
    ProcessingExecutionError,
    ProcessingLifecycleError,
    ProcessingStorageReadError,
    ProcessingStorageWriteError,
)
from atlas_processing.execution.executor import TransformationExecutor
from atlas_processing.models.processing_request import ProcessingRequest
from atlas_processing.models.processing_result import (
    ProcessingResult,
    ProcessingStatistics,
)
from atlas_processing.tracking.execution_tracker import (
    ExecutionTracker,
    ProcessingExecutionRecord,
    ProcessingState,
)


def create_default_storage_registry() -> StorageRegistry:
    """Create a :class:`StorageRegistry` populated with standard storage backends."""
    registry = StorageRegistry()
    registry.register("filesystem", FileSystemBackend)
    return registry


class ProcessingPipeline:
    """Coordinates validation, storage reads, transformations, curated writes, and tracking."""

    def __init__(
        self,
        executor: TransformationExecutor | None = None,
        storage_manager: StorageManager | None = None,
        tracker: ExecutionTracker | None = None,
        *,
        storage_provider: StorageProvider | None = None,
        storage_registry: StorageRegistry | None = None,
        backend_name: str = "filesystem",
    ) -> None:
        self._executor = executor or TransformationExecutor()
        self._tracker = tracker or ExecutionTracker()
        self._logger = get_logger("atlas.processing.pipeline")

        if storage_manager is not None:
            self._storage_manager = storage_manager
        elif storage_provider is not None:
            self._storage_manager = StorageManager(backend=storage_provider)
        else:
            registry = storage_registry or create_default_storage_registry()
            backend_cls = registry.get(backend_name)
            self._storage_manager = StorageManager(backend=backend_cls())

    @property
    def tracker(self) -> ExecutionTracker:
        """Return the pipeline's :class:`ExecutionTracker`."""
        return self._tracker

    @property
    def executor(self) -> TransformationExecutor:
        """Return the pipeline's :class:`TransformationExecutor`."""
        return self._executor

    def run(
        self,
        request: ProcessingRequest,
        *,
        execution_id: str | None = None,
        raise_on_error: bool | None = None,
    ) -> ProcessingResult:
        """Execute the full processing lifecycle for ``request``."""
        self._validate_request(request)
        should_raise = (
            raise_on_error
            if raise_on_error is not None
            else request.execution_config.raise_on_error
        )

        record = self._tracker.create_execution(request, execution_id=execution_id)
        exec_id = record.id
        combined_metadata: dict[str, Any] = dict(request.metadata)
        input_artifact: Artifact | None = None

        self._logger.info(
            "processing_execution_created",
            request_id=request.request_id,
            execution_id=exec_id,
            dataset_id=str(request.dataset_id),
            source_path=request.source.path,
            destination_path=request.destination.path,
        )

        try:
            self._tracker.start(exec_id)

            # 1. Read source dataset
            self._tracker.record_transition(exec_id, ProcessingState.READING)
            raw_dataset = self._read_source_dataset(request, exec_id)
            input_artifact = Artifact(
                id=generate_execution_id(),
                dataset_id=str(request.dataset_id),
                path=request.source.path,
                format=request.source.format,
            )

            # 2. Execute transformations
            self._tracker.record_transition(exec_id, ProcessingState.TRANSFORMING)
            exec_output = self._executor.execute(raw_dataset, request.transformations)
            if request.execution_config.fail_on_empty_input and exec_output.input_row_count == 0:
                raise ProcessingExecutionError(
                    f"Source dataset '{request.source.path}' is empty and "
                    "'fail_on_empty_input' is enabled.",
                    request_id=request.request_id,
                    execution_id=exec_id,
                )

            combined_metadata["engine"] = self._executor.engine.engine_name
            combined_metadata["step_metrics"] = [m.model_dump() for m in exec_output.step_metrics]

            # 3. Persist output dataset
            self._tracker.record_transition(exec_id, ProcessingState.WRITING)
            output_artifact = self._write_output_dataset(
                request=request,
                execution_id=exec_id,
                data=exec_output.data,
                partition_timestamp=record.started_at,
            )

            # 4. Complete execution tracking
            self._tracker.complete(
                exec_id,
                input_row_count=exec_output.input_row_count,
                output_row_count=exec_output.output_row_count,
                transformations_applied=exec_output.steps_executed,
                input_artifact_id=input_artifact.id,
                output_artifact_id=output_artifact.id,
            )
            completed_record = self._tracker.get_execution(exec_id)

            stats = ProcessingStatistics(
                input_row_count=exec_output.input_row_count,
                output_row_count=exec_output.output_row_count,
                input_column_count=len(exec_output.input_columns),
                output_column_count=len(exec_output.output_columns),
                transformations_applied=exec_output.steps_executed,
                duration_seconds=completed_record.duration_seconds,
            )

            self._logger.info(
                "processing_execution_succeeded",
                request_id=request.request_id,
                execution_id=exec_id,
                input_rows=stats.input_row_count,
                output_rows=stats.output_row_count,
                transformations_applied=stats.transformations_applied,
            )

            return ProcessingResult(
                request_id=request.request_id,
                execution_id=exec_id,
                dataset_id=request.dataset_id,
                status=ExecutionStatus.SUCCEEDED,
                lifecycle_state=completed_record.lifecycle_state,
                input_artifact=input_artifact,
                output_artifact=output_artifact,
                statistics=stats,
                started_at=completed_record.started_at,
                completed_at=completed_record.completed_at,
                duration_seconds=completed_record.duration_seconds,
                message="Processing completed successfully.",
                metadata=combined_metadata,
            )
        except Exception as exc:
            return self._handle_failure(
                request=request,
                initial_record=record,
                input_artifact=input_artifact,
                error=exc,
                metadata=combined_metadata,
                should_raise=should_raise,
            )

    @staticmethod
    def _validate_request(request: ProcessingRequest) -> None:
        if not isinstance(request, ProcessingRequest):
            raise InvalidProcessingRequestError(
                f"Expected ProcessingRequest instance, got {type(request).__name__}."
            )
        request.validate_request()

    def _read_source_dataset(
        self,
        request: ProcessingRequest,
        execution_id: str,
    ) -> Any:
        try:
            return self._storage_manager.load(request.source)
        except ProcessingStorageReadError:
            raise
        except StorageError as exc:
            raise ProcessingStorageReadError(
                f"Failed to read source dataset '{request.dataset_id}' from "
                f"'{request.source.path}': {exc}",
                request_id=request.request_id,
                execution_id=execution_id,
            ) from exc
        except Exception as exc:
            raise ProcessingStorageReadError(
                f"Unexpected error reading source dataset '{request.dataset_id}' from "
                f"'{request.source.path}': {exc}",
                request_id=request.request_id,
                execution_id=execution_id,
            ) from exc

    def _write_output_dataset(
        self,
        *,
        request: ProcessingRequest,
        execution_id: str,
        data: Any,
        partition_timestamp: datetime,
    ) -> Artifact:
        target_location = self._resolve_destination_location(request, partition_timestamp)
        try:
            if not request.execution_config.overwrite and self._storage_manager.exists(
                target_location
            ):
                raise ProcessingStorageWriteError(
                    f"Destination path '{target_location.path}' already exists and "
                    "overwrite is disabled.",
                    request_id=request.request_id,
                    execution_id=execution_id,
                )

            storage_result = self._storage_manager.save(
                data=data,
                location=target_location,
            )
        except ProcessingStorageWriteError:
            raise
        except StorageError as exc:
            raise ProcessingStorageWriteError(
                f"Storage error while persisting processed dataset '{request.dataset_id}' "
                f"to '{target_location.path}': {exc}",
                request_id=request.request_id,
                execution_id=execution_id,
            ) from exc
        except Exception as exc:
            raise ProcessingStorageWriteError(
                f"Unexpected error persisting processed dataset '{request.dataset_id}' "
                f"to '{target_location.path}': {exc}",
                request_id=request.request_id,
                execution_id=execution_id,
            ) from exc

        if not storage_result.success:
            raise ProcessingStorageWriteError(
                storage_result.message
                or f"Storage manager reported unsuccessful save for '{target_location.path}'.",
                request_id=request.request_id,
                execution_id=execution_id,
            )

        return Artifact(
            id=generate_execution_id(),
            dataset_id=str(request.dataset_id),
            path=storage_result.path,
            format=target_location.format,
        )

    @staticmethod
    def _resolve_destination_location(
        request: ProcessingRequest,
        partition_timestamp: datetime,
    ) -> StorageLocation:
        dest = request.destination
        target_path = dest.path
        if request.execution_config.partition_by_date:
            partition_segment = PartitionStrategy.by_date(partition_timestamp)
            pure = PurePath(dest.path)
            if pure.suffix:
                target_path = str(pure.parent / partition_segment / pure.name)
            else:
                target_path = str(pure / partition_segment)

        return StorageLocation(
            layer=dest.layer,
            path=target_path,
            format=dest.format,
        )

    def _handle_failure(
        self,
        *,
        request: ProcessingRequest,
        initial_record: ProcessingExecutionRecord,
        input_artifact: Artifact | None,
        error: Exception,
        metadata: dict[str, Any],
        should_raise: bool,
    ) -> ProcessingResult:
        exec_id = initial_record.id
        if isinstance(error, ProcessingError):
            error.request_id = error.request_id or request.request_id
            error.execution_id = error.execution_id or exec_id
            processing_error: ProcessingError = error
        else:
            processing_error = ProcessingError(
                f"Unexpected processing error: {error}",
                request_id=request.request_id,
                execution_id=exec_id,
            )
            processing_error.__cause__ = error

        try:
            self._tracker.fail(
                exec_id,
                reason=str(processing_error),
                error_type=type(processing_error).__name__,
            )
            failed_record = self._tracker.get_execution(exec_id)
            completed_at = failed_record.completed_at or datetime.now(UTC)
            duration_seconds = failed_record.duration_seconds
        except ProcessingLifecycleError as tracking_exc:
            # Never mask the original transformation/storage error if failure tracking fails.
            self._logger.error(
                "processing_failure_tracking_error",
                request_id=request.request_id,
                execution_id=exec_id,
                tracking_error=str(tracking_exc),
                original_error=str(processing_error),
            )
            metadata["tracking_error"] = str(tracking_exc)
            completed_at = datetime.now(UTC)
            duration_seconds = max(0.0, (completed_at - initial_record.started_at).total_seconds())

        cause = processing_error.__cause__
        error_cause = f"{type(cause).__name__}: {cause}" if cause is not None else None

        self._logger.error(
            "processing_execution_failed",
            request_id=request.request_id,
            execution_id=exec_id,
            error_type=type(processing_error).__name__,
            error_message=str(processing_error),
            error_cause=error_cause,
        )

        if should_raise:
            raise processing_error

        return ProcessingResult(
            request_id=request.request_id,
            execution_id=exec_id,
            dataset_id=request.dataset_id,
            status=ExecutionStatus.FAILED,
            lifecycle_state=ProcessingState.FAILED,
            input_artifact=input_artifact,
            output_artifact=None,
            statistics=ProcessingStatistics(duration_seconds=duration_seconds),
            started_at=initial_record.started_at,
            completed_at=completed_at,
            duration_seconds=duration_seconds,
            error_type=type(processing_error).__name__,
            error_message=str(processing_error),
            error_cause=error_cause,
            message=str(processing_error),
            metadata=metadata,
        )


__all__ = [
    "ProcessingPipeline",
    "create_default_storage_registry",
]
