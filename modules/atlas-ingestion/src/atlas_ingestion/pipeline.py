"""Coordinates ingestion extraction, loading, and tracking."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from atlas_core.enums import ExecutionStatus
from atlas_core.logging import get_logger

from atlas_ingestion.exceptions import (
    ExecutionTrackingError,
    IngestionError,
    InvalidIngestionRequestError,
)
from atlas_ingestion.extraction.extractor import Extractor
from atlas_ingestion.lifecycle import IngestionState
from atlas_ingestion.loading.loader import Loader
from atlas_ingestion.models.ingestion_request import IngestionRequest
from atlas_ingestion.models.ingestion_result import IngestionResult
from atlas_ingestion.tracking.execution_tracker import (
    ExecutionTracker,
    IngestionExecutionRecord,
)


class IngestionPipeline:
    """Coordinates validation, extraction, raw layer loading, and execution tracking."""

    def __init__(
        self,
        extractor: Extractor | None = None,
        loader: Loader | None = None,
        tracker: ExecutionTracker | None = None,
    ) -> None:
        self._extractor = extractor or Extractor()
        self._loader = loader or Loader()
        self._tracker = tracker or ExecutionTracker()
        self._logger = get_logger("atlas.ingestion.pipeline")

    @property
    def tracker(self) -> ExecutionTracker:
        """Return the pipeline's :class:`ExecutionTracker`."""
        return self._tracker

    def run(
        self,
        request: IngestionRequest,
        *,
        execution_id: str | None = None,
        raise_on_error: bool | None = None,
    ) -> IngestionResult:
        """Execute the full ingestion lifecycle for ``request``."""
        self._validate_request(request)
        should_raise = (
            raise_on_error
            if raise_on_error is not None
            else request.execution_config.raise_on_error
        )

        record = self._tracker.create_execution(request, execution_id=execution_id)
        exec_id = record.id
        combined_metadata: dict[str, Any] = dict(request.metadata)

        self._logger.info(
            "ingestion_execution_created",
            request_id=request.request_id,
            execution_id=exec_id,
            dataset_id=str(request.dataset_id),
            source_id=request.source.source_id,
        )

        try:
            self._tracker.start(exec_id)

            self._tracker.record_transition(exec_id, IngestionState.EXTRACTING)
            extraction_result = self._extractor.extract(request)
            source_meta = self._extractor.build_source_metadata(request, extraction_result)
            combined_metadata.update(source_meta)
            self._tracker.record_transition(exec_id, IngestionState.EXTRACTED)

            self._tracker.record_transition(exec_id, IngestionState.LOADING)
            artifacts = self._loader.load(
                request,
                extraction_result,
                execution_timestamp=record.started_at,
            )

            self._tracker.complete(
                exec_id,
                row_count=extraction_result.row_count,
                artifact_ids=[artifact.id for artifact in artifacts],
            )
            completed_record = self._tracker.get_execution(exec_id)

            self._logger.info(
                "ingestion_execution_succeeded",
                request_id=request.request_id,
                execution_id=exec_id,
                row_count=extraction_result.row_count,
                artifact_count=len(artifacts),
            )

            return IngestionResult(
                request_id=request.request_id,
                execution_id=exec_id,
                status=ExecutionStatus.SUCCEEDED,
                lifecycle_state=completed_record.lifecycle_state,
                source=request.to_core_source(),
                dataset_id=request.dataset_id,
                destination=request.destination,
                artifacts=artifacts,
                row_count=extraction_result.row_count,
                started_at=completed_record.started_at,
                completed_at=completed_record.completed_at,
                duration_seconds=completed_record.duration_seconds,
                message="Ingestion completed successfully.",
                metadata=combined_metadata,
            )
        except Exception as exc:
            return self._handle_failure(
                request=request,
                initial_record=record,
                error=exc,
                metadata=combined_metadata,
                should_raise=should_raise,
            )

    @staticmethod
    def _validate_request(request: IngestionRequest) -> None:
        if not isinstance(request, IngestionRequest):
            raise InvalidIngestionRequestError(
                f"Expected IngestionRequest instance, got {type(request).__name__}."
            )
        request.validate_request()

    def _handle_failure(
        self,
        *,
        request: IngestionRequest,
        initial_record: IngestionExecutionRecord,
        error: Exception,
        metadata: dict[str, Any],
        should_raise: bool,
    ) -> IngestionResult:
        exec_id = initial_record.id
        if isinstance(error, IngestionError):
            error.request_id = error.request_id or request.request_id
            error.execution_id = error.execution_id or exec_id
            ingestion_error: IngestionError = error
        else:
            ingestion_error = IngestionError(
                f"Unexpected ingestion error: {error}",
                request_id=request.request_id,
                execution_id=exec_id,
            )
            ingestion_error.__cause__ = error

        try:
            self._tracker.fail(
                exec_id,
                reason=str(ingestion_error),
                error_type=type(ingestion_error).__name__,
            )
            failed_record = self._tracker.get_execution(exec_id)
            completed_at = failed_record.completed_at or datetime.now(UTC)
            duration_seconds = failed_record.duration_seconds
        except ExecutionTrackingError as tracking_exc:
            # Never silently mask the original extraction/loading error when failure tracking fails.
            self._logger.error(
                "ingestion_failure_tracking_error",
                request_id=request.request_id,
                execution_id=exec_id,
                tracking_error=str(tracking_exc),
                original_error=str(ingestion_error),
            )
            metadata["tracking_error"] = str(tracking_exc)
            completed_at = datetime.now(UTC)
            duration_seconds = max(0.0, (completed_at - initial_record.started_at).total_seconds())

        cause = ingestion_error.__cause__
        error_cause = f"{type(cause).__name__}: {cause}" if cause is not None else None

        self._logger.error(
            "ingestion_execution_failed",
            request_id=request.request_id,
            execution_id=exec_id,
            error_type=type(ingestion_error).__name__,
            error_message=str(ingestion_error),
            error_cause=error_cause,
        )

        if should_raise:
            raise ingestion_error

        return IngestionResult(
            request_id=request.request_id,
            execution_id=exec_id,
            status=ExecutionStatus.FAILED,
            lifecycle_state=IngestionState.FAILED,
            source=request.to_core_source(),
            dataset_id=request.dataset_id,
            destination=request.destination,
            artifacts=[],
            row_count=None,
            started_at=initial_record.started_at,
            completed_at=completed_at,
            duration_seconds=duration_seconds,
            error_type=type(ingestion_error).__name__,
            error_message=str(ingestion_error),
            error_cause=error_cause,
            message=str(ingestion_error),
            metadata=metadata,
        )


__all__ = [
    "IngestionPipeline",
]
