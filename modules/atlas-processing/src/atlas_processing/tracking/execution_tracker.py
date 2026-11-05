"""Tracks processing execution state, lifecycle transitions, and outcomes."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from atlas_core import (
    DatasetId,
    Execution,
    ExecutionManager,
    ExecutionStatus,
    generate_execution_id,
)
from pydantic import Field

from atlas_processing.exceptions import ProcessingLifecycleError
from atlas_processing.models.processing_request import ProcessingRequest


class ProcessingState(StrEnum):
    """Discrete lifecycle states of a single processing operation."""

    PENDING = "pending"
    READING = "reading"
    TRANSFORMING = "transforming"
    WRITING = "writing"
    COMPLETED = "completed"
    FAILED = "failed"


VALID_PROCESSING_TRANSITIONS: dict[ProcessingState, frozenset[ProcessingState]] = {
    ProcessingState.PENDING: frozenset({ProcessingState.READING, ProcessingState.FAILED}),
    ProcessingState.READING: frozenset({ProcessingState.TRANSFORMING, ProcessingState.FAILED}),
    ProcessingState.TRANSFORMING: frozenset({ProcessingState.WRITING, ProcessingState.FAILED}),
    ProcessingState.WRITING: frozenset({ProcessingState.COMPLETED, ProcessingState.FAILED}),
    ProcessingState.COMPLETED: frozenset(),
    ProcessingState.FAILED: frozenset(),
}

TERMINAL_PROCESSING_STATES: frozenset[ProcessingState] = frozenset(
    {
        ProcessingState.COMPLETED,
        ProcessingState.FAILED,
    }
)


class ProcessingLifecycle:
    """State machine enforcing valid transitions for a processing execution."""

    def __init__(self, initial_state: ProcessingState = ProcessingState.PENDING) -> None:
        if not isinstance(initial_state, ProcessingState):
            raise ProcessingLifecycleError(f"Invalid initial processing state: {initial_state!r}")
        self._state: ProcessingState = initial_state
        self._history: list[ProcessingState] = [initial_state]

    @property
    def state(self) -> ProcessingState:
        """Current processing lifecycle state."""
        return self._state

    @property
    def history(self) -> tuple[ProcessingState, ...]:
        """Ordered sequence of states visited by this lifecycle instance."""
        return tuple(self._history)

    @property
    def is_terminal(self) -> bool:
        """Return True if the lifecycle has reached a terminal state."""
        return self._state in TERMINAL_PROCESSING_STATES

    def can_transition_to(self, target: ProcessingState) -> bool:
        """Return True if transitioning from the current state to ``target`` is valid."""
        allowed = VALID_PROCESSING_TRANSITIONS.get(self._state, frozenset())
        return target in allowed

    def transition_to(self, target: ProcessingState) -> ProcessingState:
        """Transition to ``target`` state or raise :class:`ProcessingLifecycleError`."""
        if not isinstance(target, ProcessingState):
            raise ProcessingLifecycleError(
                f"Target state must be a ProcessingState, got {target!r}."
            )
        if not self.can_transition_to(target):
            raise ProcessingLifecycleError(
                f"Invalid processing lifecycle transition from '{self._state}' to '{target}'."
            )
        self._state = target
        self._history.append(target)
        return self._state

    def to_execution_status(self) -> ExecutionStatus:
        """Map the current processing lifecycle state to core :class:`ExecutionStatus`."""
        if self._state == ProcessingState.PENDING:
            return ExecutionStatus.PENDING
        if self._state == ProcessingState.COMPLETED:
            return ExecutionStatus.SUCCEEDED
        if self._state == ProcessingState.FAILED:
            return ExecutionStatus.FAILED
        return ExecutionStatus.RUNNING


class ProcessingExecutionRecord(Execution):
    """Detailed execution record for a processing run.

    Extends :class:`atlas_core.Execution`.
    """

    request_id: str
    dataset_id: DatasetId
    status: ExecutionStatus = ExecutionStatus.PENDING
    lifecycle_state: ProcessingState = ProcessingState.PENDING
    state_history: list[ProcessingState] = Field(default_factory=lambda: [ProcessingState.PENDING])
    started_at: datetime
    completed_at: datetime | None = None
    duration_seconds: float | None = None
    input_row_count: int | None = None
    output_row_count: int | None = None
    transformations_applied: int = 0
    input_artifact_id: str | None = None
    output_artifact_id: str | None = None
    error_type: str | None = None
    error_message: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ExecutionTracker(ExecutionManager):
    """Database-independent execution tracker implementing :class:`ExecutionManager`."""

    def __init__(self, delegate: ExecutionManager | None = None) -> None:
        self._records: dict[str, ProcessingExecutionRecord] = {}
        self._lifecycles: dict[str, ProcessingLifecycle] = {}
        self._delegate = delegate

    def create_execution(
        self,
        request: ProcessingRequest,
        *,
        execution_id: str | None = None,
        started_at: datetime | None = None,
    ) -> ProcessingExecutionRecord:
        """Create and register a new :class:`ProcessingExecutionRecord` in PENDING state."""
        exec_id = execution_id or generate_execution_id()
        if not exec_id.strip():
            raise ProcessingLifecycleError(
                "Execution identifier cannot be empty.",
                request_id=request.request_id,
            )
        if exec_id in self._records:
            raise ProcessingLifecycleError(
                f"Execution '{exec_id}' already exists.",
                request_id=request.request_id,
                execution_id=exec_id,
            )

        lifecycle = ProcessingLifecycle(ProcessingState.PENDING)
        timestamp = started_at or datetime.now(UTC)

        record = ProcessingExecutionRecord(
            id=exec_id,
            request_id=request.request_id,
            dataset_id=request.dataset_id,
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
            raise ProcessingLifecycleError(
                f"Cannot start execution '{execution_id}' in terminal status '{record.status}'.",
                request_id=record.request_id,
                execution_id=execution_id,
            )
        record.status = ExecutionStatus.RUNNING
        if self._delegate is not None:
            try:
                self._delegate.start(execution_id)
            except Exception as exc:
                raise ProcessingLifecycleError(
                    f"Delegate ExecutionManager failed to start '{execution_id}': {exc}",
                    request_id=record.request_id,
                    execution_id=execution_id,
                ) from exc

    def record_transition(
        self,
        execution_id: str,
        target_state: ProcessingState,
    ) -> ProcessingExecutionRecord:
        """Validate and record a lifecycle transition for ``execution_id``."""
        record = self._get_mutable_record(execution_id)
        lifecycle = self._lifecycles[execution_id]
        try:
            lifecycle.transition_to(target_state)
        except Exception as exc:
            raise ProcessingLifecycleError(
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
        input_row_count: int | None = None,
        output_row_count: int | None = None,
        transformations_applied: int | None = None,
        input_artifact_id: str | None = None,
        output_artifact_id: str | None = None,
        completed_at: datetime | None = None,
    ) -> None:
        """Mark ``execution_id`` as completed and record metrics and completion timestamp."""
        record = self._get_mutable_record(execution_id)
        lifecycle = self._lifecycles[execution_id]

        if lifecycle.state != ProcessingState.COMPLETED:
            try:
                lifecycle.transition_to(ProcessingState.COMPLETED)
            except Exception as exc:
                raise ProcessingLifecycleError(
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
        if input_row_count is not None:
            record.input_row_count = input_row_count
        if output_row_count is not None:
            record.output_row_count = output_row_count
        if transformations_applied is not None:
            record.transformations_applied = transformations_applied
        if input_artifact_id is not None:
            record.input_artifact_id = input_artifact_id
        if output_artifact_id is not None:
            record.output_artifact_id = output_artifact_id

        if self._delegate is not None:
            try:
                self._delegate.complete(execution_id)
            except Exception as exc:
                raise ProcessingLifecycleError(
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

        if lifecycle.state != ProcessingState.FAILED:
            if lifecycle.state == ProcessingState.COMPLETED:
                raise ProcessingLifecycleError(
                    f"Cannot fail an already completed execution '{execution_id}'.",
                    request_id=record.request_id,
                    execution_id=execution_id,
                )
            try:
                lifecycle.transition_to(ProcessingState.FAILED)
            except Exception as exc:
                raise ProcessingLifecycleError(
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
        record.error_type = error_type or "ProcessingError"

        if self._delegate is not None:
            try:
                self._delegate.fail(execution_id, reason)
            except Exception as exc:
                raise ProcessingLifecycleError(
                    "Delegate ExecutionManager failed to record failure for "
                    f"'{execution_id}': {exc}",
                    request_id=record.request_id,
                    execution_id=execution_id,
                ) from exc

    def get_execution(self, execution_id: str) -> ProcessingExecutionRecord:
        """Return a snapshot of the execution record for ``execution_id``."""
        record = self._get_mutable_record(execution_id)
        return record.model_copy(deep=True)

    def list_executions(self) -> list[ProcessingExecutionRecord]:
        """Return snapshots of all tracked execution records in creation order."""
        return [rec.model_copy(deep=True) for rec in self._records.values()]

    def _get_mutable_record(self, execution_id: str) -> ProcessingExecutionRecord:
        record = self._records.get(execution_id)
        if record is None:
            raise ProcessingLifecycleError(
                f"Execution '{execution_id}' not found in tracker.",
                execution_id=execution_id,
            )
        return record


__all__ = [
    "ExecutionTracker",
    "ProcessingExecutionRecord",
    "ProcessingLifecycle",
    "ProcessingState",
    "TERMINAL_PROCESSING_STATES",
    "VALID_PROCESSING_TRANSITIONS",
]
