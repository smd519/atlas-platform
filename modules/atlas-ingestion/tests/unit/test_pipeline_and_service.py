"""Unit tests for IngestionPipeline and IngestionService."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from atlas_connectors import (
    ConnectorConnectionError,
    ExtractionResult,
    SourceConfig,
)
from atlas_core import Artifact, DatasetId, ExecutionStatus
from atlas_ingestion.exceptions import (
    ExecutionTrackingError,
    ExtractionError,
    LoadingError,
)
from atlas_ingestion.extraction.extractor import Extractor
from atlas_ingestion.lifecycle import IngestionState
from atlas_ingestion.loading.loader import Loader
from atlas_ingestion.models.ingestion_request import IngestionRequest
from atlas_ingestion.pipeline import IngestionPipeline
from atlas_ingestion.service import IngestionService
from atlas_ingestion.tracking.execution_tracker import ExecutionTracker
from atlas_storage import StorageLocation, StorageWriteError


class StubExtractor(Extractor):
    """Controllable Extractor stub for pipeline unit tests."""

    def __init__(self, *, fail: bool = False) -> None:
        self._fail = fail

    def extract(self, request: IngestionRequest) -> ExtractionResult:
        if self._fail:
            cause = ConnectorConnectionError("connection refused")
            err = ExtractionError(
                "Failed to extract from source",
                request_id=request.request_id,
            )
            err.__cause__ = cause
            raise err
        return ExtractionResult(
            source_id=request.source.source_id,
            row_count=5,
            extracted_at=datetime(2026, 4, 1, 12, 0, tzinfo=UTC),
            data=[{"id": i} for i in range(5)],
        )


class StubLoader(Loader):
    """Controllable Loader stub for pipeline unit tests."""

    def __init__(self, *, fail: bool = False) -> None:
        self._fail = fail

    def load(
        self,
        request: IngestionRequest,
        extraction_result: ExtractionResult,
        *,
        execution_timestamp: datetime | None = None,
    ) -> list[Artifact]:
        if self._fail:
            cause = StorageWriteError("disk full")
            err = LoadingError(
                "Failed to write raw data",
                request_id=request.request_id,
            )
            err.__cause__ = cause
            raise err
        return [
            Artifact(
                id="art-stub-1",
                dataset_id=str(request.dataset_id),
                path=request.destination.path,
                format=request.destination.format,
            )
        ]


class FailingFailTracker(ExecutionTracker):
    """Tracker stub whose fail() method raises an ExecutionTrackingError."""

    def fail(
        self,
        execution_id: str,
        reason: str,
        *,
        error_type: str | None = None,
        completed_at: datetime | None = None,
    ) -> None:
        raise ExecutionTrackingError("tracker storage unavailable")


def _sample_request() -> IngestionRequest:
    return IngestionRequest(
        request_id="req-pipe-1",
        dataset_id=DatasetId("ds-pipe"),
        source=SourceConfig(
            source_id="src-pipe",
            source_name="pipe_source",
            source_type="local_file",
            config={"path": "sample.csv"},
        ),
        destination=StorageLocation(
            layer="bronze",
            path="data/bronze/pipe.parquet",
            format="parquet",
        ),
        source_format="csv",
    )


def test_pipeline_success_end_to_end() -> None:
    tracker = ExecutionTracker()
    pipeline = IngestionPipeline(
        extractor=StubExtractor(),
        loader=StubLoader(),
        tracker=tracker,
    )

    result = pipeline.run(_sample_request(), execution_id="exec-pipe-ok")

    assert result.success is True
    assert result.status == ExecutionStatus.SUCCEEDED
    assert result.lifecycle_state == IngestionState.COMPLETED
    assert result.execution_id == "exec-pipe-ok"
    assert result.row_count == 5
    assert len(result.artifacts) == 1
    assert result.artifacts[0].id == "art-stub-1"
    assert result.duration_seconds is not None

    record = tracker.get_execution("exec-pipe-ok")
    assert record.status == ExecutionStatus.SUCCEEDED
    assert record.state_history == [
        IngestionState.PENDING,
        IngestionState.EXTRACTING,
        IngestionState.EXTRACTED,
        IngestionState.LOADING,
        IngestionState.COMPLETED,
    ]


def test_pipeline_extraction_failure_records_failed_execution() -> None:
    tracker = ExecutionTracker()
    pipeline = IngestionPipeline(
        extractor=StubExtractor(fail=True),
        loader=StubLoader(),
        tracker=tracker,
    )

    result = pipeline.run(_sample_request(), execution_id="exec-ext-fail")

    assert result.success is False
    assert result.status == ExecutionStatus.FAILED
    assert result.lifecycle_state == IngestionState.FAILED
    assert result.execution_id == "exec-ext-fail"
    assert result.artifacts == []
    assert result.error_type == "ExtractionError"
    assert result.error_message == "Failed to extract from source"
    assert result.error_cause == "ConnectorConnectionError: connection refused"

    record = tracker.get_execution("exec-ext-fail")
    assert record.status == ExecutionStatus.FAILED
    assert record.state_history == [
        IngestionState.PENDING,
        IngestionState.EXTRACTING,
        IngestionState.FAILED,
    ]


def test_pipeline_loading_failure_records_failed_execution() -> None:
    tracker = ExecutionTracker()
    pipeline = IngestionPipeline(
        extractor=StubExtractor(fail=False),
        loader=StubLoader(fail=True),
        tracker=tracker,
    )

    result = pipeline.run(_sample_request(), execution_id="exec-load-fail")

    assert result.success is False
    assert result.status == ExecutionStatus.FAILED
    assert result.lifecycle_state == IngestionState.FAILED
    assert result.execution_id == "exec-load-fail"
    assert result.artifacts == []
    assert result.error_type == "LoadingError"
    assert result.error_cause == "StorageWriteError: disk full"

    record = tracker.get_execution("exec-load-fail")
    assert record.status == ExecutionStatus.FAILED
    assert record.state_history == [
        IngestionState.PENDING,
        IngestionState.EXTRACTING,
        IngestionState.EXTRACTED,
        IngestionState.LOADING,
        IngestionState.FAILED,
    ]


def test_pipeline_raise_on_error_preserves_original_exception_and_execution_id() -> None:
    tracker = ExecutionTracker()
    pipeline = IngestionPipeline(
        extractor=StubExtractor(fail=True),
        loader=StubLoader(),
        tracker=tracker,
    )

    with pytest.raises(ExtractionError, match="Failed to extract from source") as exc_info:
        pipeline.run(_sample_request(), execution_id="exec-raise-1", raise_on_error=True)

    assert exc_info.value.execution_id == "exec-raise-1"
    assert isinstance(exc_info.value.__cause__, ConnectorConnectionError)
    assert tracker.get_execution("exec-raise-1").status == ExecutionStatus.FAILED


def test_pipeline_failure_tracking_error_does_not_mask_original_error() -> None:
    tracker = FailingFailTracker()
    pipeline = IngestionPipeline(
        extractor=StubExtractor(fail=True),
        loader=StubLoader(),
        tracker=tracker,
    )

    result = pipeline.run(_sample_request(), execution_id="exec-mask-check")
    assert result.status == ExecutionStatus.FAILED
    assert result.error_type == "ExtractionError"
    assert result.error_message == "Failed to extract from source"
    assert "tracker storage unavailable" in result.metadata.get("tracking_error", "")

    with pytest.raises(ExtractionError, match="Failed to extract from source"):
        pipeline.run(_sample_request(), execution_id="exec-mask-raise", raise_on_error=True)


def test_ingestion_service_delegates_to_pipeline_and_exposes_executions() -> None:
    service = IngestionService(
        extractor=StubExtractor(),
        loader=StubLoader(),
    )

    result = service.ingest(_sample_request(), execution_id="exec-svc-1")
    assert result.success is True
    assert result.execution_id == "exec-svc-1"

    record = service.get_execution("exec-svc-1")
    assert record.id == "exec-svc-1"
    assert record.status == ExecutionStatus.SUCCEEDED
    assert len(service.list_executions()) == 1


def test_staging_cleanup_failure_does_not_overwrite_successful_ingestion_result(
    tmp_path: Path,
) -> None:
    staging_dir = tmp_path / "drop" / ".staging" / "exec-cleanup-err"
    staging_dir.mkdir(parents=True, exist_ok=True)
    staged_file = staging_dir / "corporations.csv"
    staged_file.write_text("id,name\n1,Acme\n", encoding="utf-8")

    class StagedStubExtractor(Extractor):
        def extract(
            self,
            request: IngestionRequest,
            *,
            execution_id: str | None = None,
            destination: Path | None = None,
        ) -> ExtractionResult:
            return ExtractionResult(
                source_id=request.source.source_id,
                row_count=1,
                extracted_at=datetime(2026, 4, 1, 12, 0, tzinfo=UTC),
                data=[{"id": 1, "name": "Acme"}],
            )

        def get_staging_directory(self, execution_id: str) -> Path | None:
            return staging_dir if execution_id == "exec-cleanup-err" else None

        def get_staged_file(self, execution_id: str) -> Path | None:
            return staged_file if execution_id == "exec-cleanup-err" else None

    def failing_rmtree(target: Path) -> Any:
        raise PermissionError(f"Simulated file lock on '{target}'")

    pipeline = IngestionPipeline(
        extractor=StagedStubExtractor(),
        loader=StubLoader(fail=False),
        tracker=ExecutionTracker(),
        rmtree_fn=failing_rmtree,
    )

    result = pipeline.run(_sample_request(), execution_id="exec-cleanup-err")
    assert result.success is True
    assert result.status == ExecutionStatus.SUCCEEDED
    assert result.lifecycle_state == IngestionState.COMPLETED
    assert len(result.artifacts) == 1
    assert "Simulated file lock" in result.metadata.get("staging_cleanup_error", "")
