"""Tracks ingestion execution state and outcomes."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from atlas_core import (
    DatasetId,
    Execution,
    ExecutionManager,
    ExecutionStatus,
    SourceId,
    generate_execution_id,
)
from pydantic import Field

from atlas_ingestion.exceptions import ExecutionTrackingError
from atlas_ingestion.lifecycle import IngestionLifecycle, IngestionState
from atlas_ingestion.models.ingestion_request import IngestionRequest


class IngestionExecutionRecord(Execution):
    """Detailed execution record for an ingestion run.

    Inherits ``id`` and ``status`` from :class:`atlas_core.models.execution.Execution`.
    """

    request_id: str
    dataset_id: DatasetId
    source_id: SourceId
    status: ExecutionStatus = ExecutionStatus.PENDING
    lifecycle_state: IngestionState = IngestionState.PENDING
    state_history: list[IngestionState] = Field(default_factory=lambda: [IngestionState.PENDING])
    started_at: datetime
    completed_at: datetime | None = None
    duration_seconds: float | None = None
    row_count: int | None = None
    artifact_ids: list[str] = Field(default_factory=list)
    error_type: str | None = None
    error_message: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ExecutionTracker(ExecutionManager):
    """Database-independent execution tracker implementing :class:`ExecutionManager`.

    Maintains in-memory execution records and lifecycle state machines while optionally
    forwarding lifecycle events (`start`, `complete`, `fail`) to an injected
    :class:`ExecutionManager` delegate.
    """

    def __init__(self, delegate: ExecutionManager | None = None) -> None:
        self._records: dict[str, IngestionExecutionRecord] = {}
        self._lifecycles: dict[str, IngestionLifecycle] = {}
        self._delegate = delegate

    def create_execution(
        self,
        request: IngestionRequest,
        *,
        execution_id: str | None = None,
        started_at: datetime | None = None,
    ) -> IngestionExecutionRecord:
        """Create and register a new :class:`IngestionExecutionRecord` in PENDING state."""
        exec_id = execution_id or generate_execution_id()
        if not exec_id.strip():
            raise ExecutionTrackingError(
                "Execution identifier cannot be empty.",
                request_id=request.request_id,
            )
        if exec_id in self._records:
            raise ExecutionTrackingError(
                f"Execution '{exec_id}' already exists.",
                request_id=request.request_id,
                execution_id=exec_id,
            )

        lifecycle = IngestionLifecycle(IngestionState.PENDING)
        timestamp = started_at or datetime.now(UTC)

        record = IngestionExecutionRecord(
            id=exec_id,
            request_id=request.request_id,
            dataset_id=request.dataset_id,
            source_id=SourceId(request.source.source_id),
            status=ExecutionStatus.PENDING,
            lifecycle_state=lifecycle.state,
            state_history=list(lifecycle.history),
            started_at=timestamp,
            metadata=dict(request.metadata),
        )
        self._lifecycles[exec_id] = lifecycle
        self._records[exec_id] = record
        return record.model_copy(deep=True)

    def start(self, execution_id: str) -> None:
        """Mark an execution as started (RUNNING) and notify optional delegate."""
        record = self._get_mutable_record(execution_id)
        if record.status in {ExecutionStatus.SUCCEEDED, ExecutionStatus.FAILED}:
            raise ExecutionTrackingError(
                f"Cannot start execution '{execution_id}' in terminal status '{record.status}'.",
                request_id=record.request_id,
                execution_id=execution_id,
            )
        record.status = ExecutionStatus.RUNNING
        if self._delegate is not None:
            try:
                self._delegate.start(execution_id)
            except Exception as exc:
                raise ExecutionTrackingError(
                    f"Delegate ExecutionManager failed to start '{execution_id}': {exc}",
                    request_id=record.request_id,
                    execution_id=execution_id,
                ) from exc

    def record_transition(
        self,
        execution_id: str,
        target_state: IngestionState,
    ) -> IngestionExecutionRecord:
        """Validate and record a lifecycle transition for ``execution_id``."""
        record = self._get_mutable_record(execution_id)
        lifecycle = self._lifecycles[execution_id]
        try:
            lifecycle.transition_to(target_state)
        except Exception as exc:
            raise ExecutionTrackingError(
                f"Failed to transition execution '{execution_id}' to '{target_state}': {exc}",
                request_id=record.request_id,
                execution_id=execution_id,
            ) from exc

        record.lifecycle_state = lifecycle.state
        record.state_history = list(lifecycle.history)
        record.status = lifecycle.to_execution_status()
        return record.model_copy(deep=True)

    def complete(
        self,
        execution_id: str,
        *,
        row_count: int | None = None,
        artifact_ids: list[str] | None = None,
        completed_at: datetime | None = None,
    ) -> None:
        """Mark ``execution_id`` as completed and record completion timestamps."""
        record = self._get_mutable_record(execution_id)
        lifecycle = self._lifecycles[execution_id]

        if lifecycle.state != IngestionState.COMPLETED:
            try:
                lifecycle.transition_to(IngestionState.COMPLETED)
            except Exception as exc:
                raise ExecutionTrackingError(
                    f"Cannot complete execution '{execution_id}' from state "
                    f"'{lifecycle.state}': {exc}",
                    request_id=record.request_id,
                    execution_id=execution_id,
                ) from exc

        finish_time = completed_at or datetime.now(UTC)
        record.lifecycle_state = lifecycle.state
        record.state_history = list(lifecycle.history)
        record.status = ExecutionStatus.SUCCEEDED
        record.completed_at = finish_time
        record.duration_seconds = max(0.0, (finish_time - record.started_at).total_seconds())
        if row_count is not None:
            record.row_count = row_count
        if artifact_ids is not None:
            record.artifact_ids = list(artifact_ids)

        if self._delegate is not None:
            try:
                self._delegate.complete(execution_id)
            except Exception as exc:
                raise ExecutionTrackingError(
                    f"Delegate ExecutionManager failed to complete '{execution_id}': {exc}",
                    request_id=record.request_id,
                    execution_id=execution_id,
                ) from exc

    def fail(
        self,
        execution_id: str,
        reason: str,
        *,
        error_type: str | None = None,
        completed_at: datetime | None = None,
    ) -> None:
        """Mark ``execution_id`` as failed and record failure reason and timestamp."""
        record = self._get_mutable_record(execution_id)
        lifecycle = self._lifecycles[execution_id]

        if lifecycle.state != IngestionState.FAILED:
            if lifecycle.state == IngestionState.COMPLETED:
                raise ExecutionTrackingError(
                    f"Cannot fail an already completed execution '{execution_id}'.",
                    request_id=record.request_id,
                    execution_id=execution_id,
                )
            try:
                lifecycle.transition_to(IngestionState.FAILED)
            except Exception as exc:
                raise ExecutionTrackingError(
                    f"Cannot transition execution '{execution_id}' to FAILED: {exc}",
                    request_id=record.request_id,
                    execution_id=execution_id,
                ) from exc

        finish_time = completed_at or datetime.now(UTC)
        record.lifecycle_state = lifecycle.state
        record.state_history = list(lifecycle.history)
        record.status = ExecutionStatus.FAILED
        record.completed_at = finish_time
        record.duration_seconds = max(0.0, (finish_time - record.started_at).total_seconds())
        record.error_message = reason
        record.error_type = error_type or "IngestionError"

        if self._delegate is not None:
            try:
                self._delegate.fail(execution_id, reason)
            except Exception as exc:
                raise ExecutionTrackingError(
                    f"Delegate ExecutionManager failed to record failure for "
                    f"'{execution_id}': {exc}",
                    request_id=record.request_id,
                    execution_id=execution_id,
                ) from exc

    def get_execution(self, execution_id: str) -> IngestionExecutionRecord:
        """Return a snapshot of the execution record for ``execution_id``."""
        record = self._get_mutable_record(execution_id)
        return record.model_copy(deep=True)

    def get_lifecycle(self, execution_id: str) -> IngestionLifecycle:
        """Return the lifecycle instance associated with ``execution_id``."""
        self._get_mutable_record(execution_id)
        return self._lifecycles[execution_id]

    def list_executions(self) -> list[IngestionExecutionRecord]:
        """Return snapshots of all tracked execution records in creation order."""
        return [rec.model_copy(deep=True) for rec in self._records.values()]

    def _get_mutable_record(self, execution_id: str) -> IngestionExecutionRecord:
        record = self._records.get(execution_id)
        if record is None:
            raise ExecutionTrackingError(
                f"Execution '{execution_id}' not found in tracker.",
                execution_id=execution_id,
            )
        return record


__all__ = [
    "ExecutionTracker",
    "IngestionExecutionRecord",
]
