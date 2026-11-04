"""Unit tests for ingestion execution tracking."""

from __future__ import annotations

import pytest
from atlas_connectors.models.source_config import SourceConfig
from atlas_core.contracts.execution import ExecutionManager
from atlas_core.enums import ExecutionStatus
from atlas_core.types import DatasetId
from atlas_ingestion.exceptions import ExecutionTrackingError
from atlas_ingestion.lifecycle import IngestionState
from atlas_ingestion.models.ingestion_request import IngestionRequest
from atlas_ingestion.tracking.execution_tracker import ExecutionTracker
from atlas_storage.models.storage_location import StorageLocation


class FakeExecutionManagerDelegate(ExecutionManager):
    """In-memory fake implementing core ExecutionManager for testing delegation."""

    def __init__(self, *, fail_on: str | None = None) -> None:
        self.started: list[str] = []
        self.completed: list[str] = []
        self.failed: list[tuple[str, str]] = []
        self._fail_on = fail_on

    def start(self, execution_id: str) -> None:
        if self._fail_on == "start":
            raise RuntimeError("delegate start failure")
        self.started.append(execution_id)

    def complete(self, execution_id: str) -> None:
        if self._fail_on == "complete":
            raise RuntimeError("delegate complete failure")
        self.completed.append(execution_id)

    def fail(self, execution_id: str, reason: str) -> None:
        if self._fail_on == "fail":
            raise RuntimeError("delegate fail failure")
        self.failed.append((execution_id, reason))


def _sample_request() -> IngestionRequest:
    return IngestionRequest(
        request_id="req-track-1",
        dataset_id=DatasetId("ds-track"),
        source=SourceConfig(
            source_id="src-track",
            source_name="track_source",
            source_type="local_file",
            config={"path": "sample.csv"},
        ),
        destination=StorageLocation(
            layer="bronze",
            path="data/bronze/track.csv",
            format="csv",
        ),
        source_format="csv",
    )


def test_execution_tracker_full_lifecycle_and_delegate() -> None:
    delegate = FakeExecutionManagerDelegate()
    tracker = ExecutionTracker(delegate=delegate)
    request = _sample_request()

    record = tracker.create_execution(request, execution_id="exec-101")
    assert record.id == "exec-101"
    assert record.status == ExecutionStatus.PENDING
    assert record.lifecycle_state == IngestionState.PENDING

    tracker.start("exec-101")
    assert delegate.started == ["exec-101"]

    tracker.record_transition("exec-101", IngestionState.EXTRACTING)
    tracker.record_transition("exec-101", IngestionState.EXTRACTED)
    tracker.record_transition("exec-101", IngestionState.LOADING)
    tracker.complete("exec-101", row_count=15, artifact_ids=["art-1"])

    final = tracker.get_execution("exec-101")
    assert final.status == ExecutionStatus.SUCCEEDED
    assert final.lifecycle_state == IngestionState.COMPLETED
    assert final.row_count == 15
    assert final.artifact_ids == ["art-1"]
    assert final.completed_at is not None
    assert final.duration_seconds is not None
    assert delegate.completed == ["exec-101"]
    assert len(tracker.list_executions()) == 1


def test_execution_tracker_records_failure() -> None:
    delegate = FakeExecutionManagerDelegate()
    tracker = ExecutionTracker(delegate=delegate)
    request = _sample_request()

    record = tracker.create_execution(request, execution_id="exec-fail-1")
    tracker.start(record.id)
    tracker.record_transition(record.id, IngestionState.EXTRACTING)
    tracker.fail(record.id, "Connector timed out", error_type="ExtractionError")

    failed = tracker.get_execution(record.id)
    assert failed.status == ExecutionStatus.FAILED
    assert failed.lifecycle_state == IngestionState.FAILED
    assert failed.error_message == "Connector timed out"
    assert failed.error_type == "ExtractionError"
    assert delegate.failed == [("exec-fail-1", "Connector timed out")]


def test_execution_tracker_rejects_duplicate_and_unknown_ids() -> None:
    tracker = ExecutionTracker()
    request = _sample_request()
    tracker.create_execution(request, execution_id="exec-dup")

    with pytest.raises(ExecutionTrackingError, match="already exists"):
        tracker.create_execution(request, execution_id="exec-dup")

    with pytest.raises(ExecutionTrackingError, match="not found"):
        tracker.get_execution("exec-missing")


def test_execution_tracker_surfaces_delegate_errors() -> None:
    delegate = FakeExecutionManagerDelegate(fail_on="start")
    tracker = ExecutionTracker(delegate=delegate)
    record = tracker.create_execution(_sample_request(), execution_id="exec-del-err")

    with pytest.raises(ExecutionTrackingError, match="Delegate ExecutionManager failed"):
        tracker.start(record.id)
