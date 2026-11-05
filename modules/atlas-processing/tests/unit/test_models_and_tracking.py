"""Unit tests for processing models, lifecycle, and execution tracking."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from atlas_core import (
    Artifact,
    DatasetId,
    ExecutionManager,
    ExecutionStatus,
)
from atlas_processing import (
    ExecutionTracker,
    InvalidProcessingRequestError,
    ProcessingExecutionConfig,
    ProcessingLifecycle,
    ProcessingLifecycleError,
    ProcessingRequest,
    ProcessingResult,
    ProcessingState,
    ProcessingStatistics,
    TransformationStep,
)
from atlas_storage import StorageLocation


class DummyExecutionManager(ExecutionManager):
    """In-memory delegate ExecutionManager for testing."""

    def __init__(self) -> None:
        self.started: list[str] = []
        self.completed: list[str] = []
        self.failed: list[tuple[str, str]] = []

    def start(self, execution_id: str) -> None:
        self.started.append(execution_id)

    def complete(self, execution_id: str) -> None:
        self.completed.append(execution_id)

    def fail(self, execution_id: str, reason: str) -> None:
        self.failed.append((execution_id, reason))


def test_processing_request_valid() -> None:
    """ProcessingRequest accepts valid configuration and normalizes formats/layers."""
    req = ProcessingRequest(
        dataset_id=DatasetId("sales_orders"),
        source=StorageLocation(layer="Bronze", path="/tmp/in.json", format="JSON"),
        destination=StorageLocation(layer="Silver", path="/tmp/out.parquet", format="PARQUET"),
        transformations=[
            TransformationStep(
                transformation_id="filter",
                params={"column": "id", "operator": "not_null"},
            ),
            {"transformation_id": "map", "params": {"select_columns": ["id"]}},
        ],
        execution_config=ProcessingExecutionConfig(
            overwrite=False,
            fail_on_empty_input=True,
            partition_by_date=True,
        ),
        metadata={"owner": "analytics"},
    )
    assert req.dataset_id == DatasetId("sales_orders")
    assert req.source.layer == "bronze"
    assert req.source.format == "json"
    assert req.destination.layer == "silver"
    assert req.destination.format == "parquet"
    assert len(req.transformations) == 2
    assert req.execution_config.overwrite is False
    assert req.execution_config.fail_on_empty_input is True
    assert req.execution_config.partition_by_date is True


def test_processing_request_invalid_dataset_or_id() -> None:
    """ProcessingRequest raises InvalidProcessingRequestError on blank IDs."""
    src = StorageLocation(layer="bronze", path="/tmp/a.csv", format="csv")
    dst = StorageLocation(layer="silver", path="/tmp/b.csv", format="csv")

    with pytest.raises(InvalidProcessingRequestError, match="dataset_id"):
        ProcessingRequest(dataset_id=DatasetId("   "), source=src, destination=dst)

    with pytest.raises(InvalidProcessingRequestError, match="request_id"):
        ProcessingRequest(
            request_id="   ",
            dataset_id=DatasetId("ds"),
            source=src,
            destination=dst,
        )


def test_processing_request_invalid_layers_and_identical_paths() -> None:
    """ProcessingRequest rejects invalid layers or identical source/destination paths."""
    with pytest.raises(InvalidProcessingRequestError, match="Invalid destination layer"):
        ProcessingRequest(
            dataset_id=DatasetId("ds"),
            source=StorageLocation(layer="bronze", path="/tmp/a.csv", format="csv"),
            destination=StorageLocation(layer="bronze", path="/tmp/b.csv", format="csv"),
        )

    with pytest.raises(InvalidProcessingRequestError, match="Invalid source layer"):
        ProcessingRequest(
            dataset_id=DatasetId("ds"),
            source=StorageLocation(layer="gold", path="/tmp/a.csv", format="csv"),
            destination=StorageLocation(layer="silver", path="/tmp/b.csv", format="csv"),
        )

    with pytest.raises(InvalidProcessingRequestError, match="must be distinct"):
        ProcessingRequest(
            dataset_id=DatasetId("ds"),
            source=StorageLocation(layer="silver", path="/tmp/same.csv", format="csv"),
            destination=StorageLocation(layer="silver", path="/tmp/same.csv", format="csv"),
        )


def test_processing_statistics_and_result() -> None:
    """ProcessingResult computes rows_delta and clears output_artifact when FAILED."""
    stats = ProcessingStatistics(
        input_row_count=10,
        output_row_count=8,
        input_column_count=2,
        output_column_count=3,
        transformations_applied=2,
    )
    assert stats.input_row_count == 10
    assert stats.output_row_count == 8
    assert stats.rows_delta == -2

    artifact = Artifact(
        id="art-1",
        dataset_id="ds-1",
        path="/tmp/out.parquet",
        format="parquet",
    )

    now = datetime.now(UTC)
    failed_res = ProcessingResult(
        request_id="req-1",
        execution_id="exec-1",
        dataset_id=DatasetId("ds-1"),
        status=ExecutionStatus.FAILED,
        lifecycle_state=ProcessingState.FAILED,
        output_artifact=artifact,
        started_at=now,
        error_message="Storage error",
    )
    assert failed_res.success is False
    assert failed_res.output_artifact is None
    assert failed_res.message == "Storage error"

    completed_res = ProcessingResult(
        request_id="req-1",
        execution_id="exec-1",
        dataset_id=DatasetId("ds-1"),
        status=ExecutionStatus.SUCCEEDED,
        lifecycle_state=ProcessingState.COMPLETED,
        output_artifact=artifact,
        statistics=stats,
        started_at=now,
        completed_at=now,
    )
    assert completed_res.success is True
    assert completed_res.output_artifact == artifact
    assert completed_res.statistics == stats


def test_processing_lifecycle_transitions() -> None:
    """ProcessingLifecycle enforces legal state transitions."""
    lifecycle = ProcessingLifecycle(ProcessingState.PENDING)
    assert lifecycle.state == ProcessingState.PENDING
    assert lifecycle.to_execution_status() == ExecutionStatus.PENDING
    assert lifecycle.can_transition_to(ProcessingState.READING)
    assert not lifecycle.can_transition_to(ProcessingState.COMPLETED)

    lifecycle.transition_to(ProcessingState.READING)
    assert lifecycle.to_execution_status() == ExecutionStatus.RUNNING
    lifecycle.transition_to(ProcessingState.TRANSFORMING)
    lifecycle.transition_to(ProcessingState.WRITING)
    lifecycle.transition_to(ProcessingState.COMPLETED)
    assert lifecycle.is_terminal is True
    assert lifecycle.to_execution_status() == ExecutionStatus.SUCCEEDED

    with pytest.raises(ProcessingLifecycleError, match="Invalid processing lifecycle transition"):
        lifecycle.transition_to(ProcessingState.READING)


def test_execution_tracker_lifecycle_and_delegate() -> None:
    """ExecutionTracker tracks processing executions and synchronizes with ExecutionManager."""
    delegate = DummyExecutionManager()
    tracker = ExecutionTracker(delegate=delegate)

    req = ProcessingRequest(
        request_id="req-100",
        dataset_id=DatasetId("orders"),
        source=StorageLocation(layer="bronze", path="/tmp/in.csv", format="csv"),
        destination=StorageLocation(layer="silver", path="/tmp/out.csv", format="csv"),
        metadata={"env": "test"},
    )
    rec = tracker.create_execution(req, execution_id="exec-100")
    assert rec.status == ExecutionStatus.PENDING
    assert rec.lifecycle_state == ProcessingState.PENDING

    tracker.start(rec.id)
    assert delegate.started == ["exec-100"]
    assert tracker.get_execution(rec.id).status == ExecutionStatus.RUNNING

    tracker.record_transition(rec.id, ProcessingState.READING)
    tracker.record_transition(rec.id, ProcessingState.TRANSFORMING)
    tracker.record_transition(rec.id, ProcessingState.WRITING)

    tracker.complete(
        rec.id,
        input_row_count=5,
        output_row_count=5,
        transformations_applied=1,
        input_artifact_id="art-in",
        output_artifact_id="art-out",
    )
    completed = tracker.get_execution(rec.id)
    assert completed.status == ExecutionStatus.SUCCEEDED
    assert completed.lifecycle_state == ProcessingState.COMPLETED
    assert completed.input_row_count == 5
    assert completed.output_row_count == 5
    assert completed.output_artifact_id == "art-out"
    assert completed.completed_at is not None
    assert delegate.completed == ["exec-100"]
    assert len(tracker.list_executions()) == 1


def test_execution_tracker_failure_and_unknown_id() -> None:
    """ExecutionTracker records failures and raises ProcessingLifecycleError on unknown IDs."""
    delegate = DummyExecutionManager()
    tracker = ExecutionTracker(delegate=delegate)
    req = ProcessingRequest(
        request_id="req-200",
        dataset_id=DatasetId("orders"),
        source=StorageLocation(layer="bronze", path="/tmp/in.csv", format="csv"),
        destination=StorageLocation(layer="silver", path="/tmp/out.csv", format="csv"),
    )
    rec = tracker.create_execution(req, execution_id="exec-200")
    tracker.start(rec.id)
    tracker.fail(rec.id, "Transformation failed", error_type="TransformationError")

    failed = tracker.get_execution(rec.id)
    assert failed.status == ExecutionStatus.FAILED
    assert failed.lifecycle_state == ProcessingState.FAILED
    assert failed.error_message == "Transformation failed"
    assert failed.error_type == "TransformationError"
    assert delegate.failed == [("exec-200", "Transformation failed")]

    with pytest.raises(ProcessingLifecycleError, match="not found in tracker"):
        tracker.get_execution("non-existent-id")
