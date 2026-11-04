"""Unit tests for ProcessingPipeline and ProcessingService."""

from __future__ import annotations

from typing import Any

import pytest
from atlas_core import DatasetId, ExecutionStatus
from atlas_processing import (
    ProcessingExecutionConfig,
    ProcessingPipeline,
    ProcessingRequest,
    ProcessingService,
    ProcessingStorageReadError,
    ProcessingStorageWriteError,
    TransformationError,
    TransformationStep,
)
from atlas_storage import (
    FileSystemBackend,
    StorageLocation,
    StorageManager,
    StorageReadError,
    StorageResult,
    StorageWriteError,
)


class StubStorageManager(StorageManager):
    """Configurable stub StorageManager for unit testing pipeline failure modes."""

    def __init__(
        self,
        data_to_return: Any = None,
        read_error: Exception | None = None,
        write_error: Exception | None = None,
        dest_exists: bool = False,
    ) -> None:
        super().__init__(backend=FileSystemBackend())
        self.data_to_return = (
            data_to_return if data_to_return is not None else [{"id": 1, "val": 10}]
        )
        self.read_error = read_error
        self.write_error = write_error
        self.dest_exists = dest_exists
        self.saved_payloads: list[tuple[Any, StorageLocation]] = []

    def load(self, location: StorageLocation) -> Any:
        if self.read_error is not None:
            raise self.read_error
        return self.data_to_return

    def exists(self, location: StorageLocation) -> bool:
        return self.dest_exists

    def save(
        self,
        data: Any,
        location: StorageLocation,
        *,
        serializer: Any = None,
    ) -> StorageResult:
        if self.write_error is not None:
            raise self.write_error
        self.saved_payloads.append((data, location))
        return StorageResult(
            success=True,
            path=location.path,
            message="ok",
        )


def test_processing_service_happy_path() -> None:
    """ProcessingService processes dataset, tracks execution, and returns ProcessingResult."""
    stub_storage = StubStorageManager(
        data_to_return=[
            {"id": 1, "status": "ok", "amount": 50},
            {"id": 2, "status": "bad", "amount": 10},
            {"id": 3, "status": "ok", "amount": 70},
        ]
    )
    service = ProcessingService(storage_manager=stub_storage)

    request = ProcessingRequest(
        dataset_id=DatasetId("payments"),
        source=StorageLocation(layer="bronze", path="/tmp/bronze/payments.json", format="json"),
        destination=StorageLocation(
            layer="silver", path="/tmp/silver/payments.parquet", format="parquet"
        ),
        transformations=[
            TransformationStep(
                name="filter_ok",
                transformation_id="filter",
                params={"column": "status", "operator": "eq", "value": "ok"},
            ),
            TransformationStep(
                name="select_cols",
                transformation_id="map",
                params={"select_columns": ["id", "amount"]},
            ),
        ],
        execution_config=ProcessingExecutionConfig(
            partition_by_date=True,
        ),
    )

    result = service.process(request)
    assert result.status == ExecutionStatus.SUCCEEDED
    assert result.success is True
    assert result.output_artifact is not None
    assert "payments.parquet" in result.output_artifact.path
    assert result.statistics.input_row_count == 3
    assert result.statistics.output_row_count == 2
    assert result.statistics.rows_delta == -1
    assert result.statistics.transformations_applied == 2
    assert result.statistics.output_column_count == 2

    # Check execution tracking
    exec_record = service.get_execution(result.execution_id)
    assert exec_record.status == ExecutionStatus.SUCCEEDED
    assert exec_record.output_artifact_id == result.output_artifact.id
    assert len(service.list_executions()) == 1


def test_processing_pipeline_storage_read_failure() -> None:
    """Pipeline records FAILED execution and wraps StorageReadError."""
    stub_storage = StubStorageManager(read_error=StorageReadError("Source file missing"))
    service = ProcessingService(storage_manager=stub_storage)
    request = ProcessingRequest(
        dataset_id=DatasetId("payments"),
        source=StorageLocation(layer="bronze", path="/tmp/missing.json", format="json"),
        destination=StorageLocation(layer="silver", path="/tmp/out.json", format="json"),
    )

    result = service.process(request)
    assert result.status == ExecutionStatus.FAILED
    assert result.success is False
    assert result.output_artifact is None
    assert result.error_message is not None
    assert "Source file missing" in result.error_message

    # With raise_on_error=True, raises ProcessingStorageReadError with chained cause
    pipeline_raising = ProcessingPipeline(storage_manager=stub_storage)
    with pytest.raises(
        ProcessingStorageReadError, match="Failed to read source dataset"
    ) as exc_info:
        pipeline_raising.run(request, raise_on_error=True)
    assert isinstance(exc_info.value.__cause__, StorageReadError)


def test_processing_pipeline_transformation_failure() -> None:
    """Pipeline records FAILED execution on transformation failure and cleans up."""
    stub_storage = StubStorageManager(data_to_return=[{"id": 1}])
    pipeline = ProcessingPipeline(storage_manager=stub_storage)
    request = ProcessingRequest(
        dataset_id=DatasetId("payments"),
        source=StorageLocation(layer="bronze", path="/tmp/in.json", format="json"),
        destination=StorageLocation(layer="silver", path="/tmp/out.json", format="json"),
        transformations=[
            TransformationStep(
                transformation_id="filter",
                params={"column": "missing_col", "operator": "eq", "value": 1},
            )
        ],
    )

    with pytest.raises(TransformationError, match="Missing required column"):
        pipeline.run(request, raise_on_error=True)
    assert stub_storage.saved_payloads == []


def test_processing_pipeline_storage_write_and_overwrite_failure() -> None:
    """Pipeline enforces overwrite=False and wraps StorageWriteError."""
    stub_exists = StubStorageManager(dest_exists=True)
    pipeline_conflict = ProcessingPipeline(storage_manager=stub_exists)
    req_no_overwrite = ProcessingRequest(
        dataset_id=DatasetId("payments"),
        source=StorageLocation(layer="bronze", path="/tmp/in.json", format="json"),
        destination=StorageLocation(layer="silver", path="/tmp/out.json", format="json"),
        execution_config=ProcessingExecutionConfig(overwrite=False),
    )

    with pytest.raises(ProcessingStorageWriteError, match="overwrite is disabled"):
        pipeline_conflict.run(req_no_overwrite, raise_on_error=True)

    stub_write_err = StubStorageManager(write_error=StorageWriteError("Disk full"))
    pipeline_write_err = ProcessingPipeline(storage_manager=stub_write_err)
    req_overwrite = ProcessingRequest(
        dataset_id=DatasetId("payments"),
        source=StorageLocation(layer="bronze", path="/tmp/in.json", format="json"),
        destination=StorageLocation(layer="silver", path="/tmp/out.json", format="json"),
    )

    with pytest.raises(
        ProcessingStorageWriteError, match="Storage error while persisting"
    ) as exc_info:
        pipeline_write_err.run(req_overwrite, raise_on_error=True)
    assert isinstance(exc_info.value.__cause__, StorageWriteError)


def test_processing_pipeline_fail_on_empty_input() -> None:
    """Pipeline respects fail_on_empty_input execution configuration."""
    stub_empty = StubStorageManager(data_to_return=[])
    service = ProcessingService(storage_manager=stub_empty)
    req = ProcessingRequest(
        dataset_id=DatasetId("empty_ds"),
        source=StorageLocation(layer="bronze", path="/tmp/in.json", format="json"),
        destination=StorageLocation(layer="silver", path="/tmp/out.json", format="json"),
        execution_config=ProcessingExecutionConfig(fail_on_empty_input=True),
    )

    res = service.process(req)
    assert res.status == ExecutionStatus.FAILED
    assert res.error_message is not None
    assert "empty" in res.error_message.lower()
