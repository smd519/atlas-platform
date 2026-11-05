"""Tracks quality assessment execution state, lifecycle transitions, and reports."""

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

from atlas_quality.exceptions import QualityExecutionTrackingError
from atlas_quality.models.quality_report import (
    QualityAssessmentStatus,
    QualityReport,
)


class QualityAssessmentState(StrEnum):
    """Discrete lifecycle states of a single quality assessment execution."""

    PENDING = "pending"
    LOADING = "loading"
    EVALUATING = "evaluating"
    PROFILING = "profiling"
    REPORTING = "reporting"
    COMPLETED = "completed"
    FAILED = "failed"


VALID_ASSESSMENT_TRANSITIONS: dict[QualityAssessmentState, frozenset[QualityAssessmentState]] = {
    QualityAssessmentState.PENDING: frozenset(
        {
            QualityAssessmentState.LOADING,
            QualityAssessmentState.EVALUATING,
            QualityAssessmentState.FAILED,
        }
    ),
    QualityAssessmentState.LOADING: frozenset(
        {
            QualityAssessmentState.EVALUATING,
            QualityAssessmentState.FAILED,
        }
    ),
    QualityAssessmentState.EVALUATING: frozenset(
        {
            QualityAssessmentState.PROFILING,
            QualityAssessmentState.REPORTING,
            QualityAssessmentState.COMPLETED,
            QualityAssessmentState.FAILED,
        }
    ),
    QualityAssessmentState.PROFILING: frozenset(
        {
            QualityAssessmentState.REPORTING,
            QualityAssessmentState.COMPLETED,
            QualityAssessmentState.FAILED,
        }
    ),
    QualityAssessmentState.REPORTING: frozenset(
        {
            QualityAssessmentState.COMPLETED,
            QualityAssessmentState.FAILED,
        }
    ),
    QualityAssessmentState.COMPLETED: frozenset(),
    QualityAssessmentState.FAILED: frozenset(),
}

TERMINAL_ASSESSMENT_STATES: frozenset[QualityAssessmentState] = frozenset(
    {
        QualityAssessmentState.COMPLETED,
        QualityAssessmentState.FAILED,
    }
)


class QualityAssessmentLifecycle:
    """State machine enforcing valid transitions for a quality assessment execution."""

    def __init__(
        self,
        initial_state: QualityAssessmentState = QualityAssessmentState.PENDING,
    ) -> None:
        if not isinstance(initial_state, QualityAssessmentState):
            raise QualityExecutionTrackingError(
                f"Invalid initial quality assessment state: {initial_state!r}"
            )
        self._state = initial_state
        self._history: list[QualityAssessmentState] = [initial_state]

    @property
    def state(self) -> QualityAssessmentState:
        """Current assessment lifecycle state."""
        return self._state

    @property
    def history(self) -> tuple[QualityAssessmentState, ...]:
        """Ordered sequence of states visited by this lifecycle instance."""
        return tuple(self._history)

    @property
    def is_terminal(self) -> bool:
        """Return True if the lifecycle is in a terminal state."""
        return self._state in TERMINAL_ASSESSMENT_STATES

    def can_transition_to(self, target: QualityAssessmentState) -> bool:
        """Return True if transitioning from the current state to ``target`` is valid."""
        allowed = VALID_ASSESSMENT_TRANSITIONS.get(self._state, frozenset())
        return target in allowed

    def transition_to(self, target: QualityAssessmentState) -> QualityAssessmentState:
        """Transition to ``target`` state or raise :class:`QualityExecutionTrackingError`."""
        if not isinstance(target, QualityAssessmentState):
            raise QualityExecutionTrackingError(
                f"Target state must be a QualityAssessmentState, got {target!r}."
            )
        if not self.can_transition_to(target):
            raise QualityExecutionTrackingError(
                f"Invalid quality assessment lifecycle transition from "
                f"'{self._state}' to '{target}'."
            )
        self._state = target
        self._history.append(target)
        return self._state

    def to_execution_status(self) -> ExecutionStatus:
        """Map the current assessment lifecycle state to core :class:`ExecutionStatus`."""
        if self._state == QualityAssessmentState.PENDING:
            return ExecutionStatus.PENDING
        if self._state == QualityAssessmentState.COMPLETED:
            return ExecutionStatus.SUCCEEDED
        if self._state == QualityAssessmentState.FAILED:
            return ExecutionStatus.FAILED
        return ExecutionStatus.RUNNING


class QualityExecutionRecord(Execution):
    """Detailed execution record for a quality assessment run.

    Extends :class:`atlas_core.Execution`.
    """

    assessment_id: str
    dataset_id: DatasetId
    status: ExecutionStatus = ExecutionStatus.PENDING
    lifecycle_state: QualityAssessmentState = QualityAssessmentState.PENDING
    state_history: list[QualityAssessmentState] = Field(
        default_factory=lambda: [QualityAssessmentState.PENDING]
    )
    overall_assessment_status: QualityAssessmentStatus | None = None
    started_at: datetime
    completed_at: datetime | None = None
    duration_seconds: float | None = None
    total_rules: int = 0
    passed_rules: int = 0
    failed_rules: int = 0
    execution_errors: int = 0
    quality_score: float | None = None
    error_type: str | None = None
    error_message: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ExecutionTracker(ExecutionManager):
    """In-memory execution tracker implementing :class:`atlas_core.ExecutionManager`."""

    def __init__(self, delegate: ExecutionManager | None = None) -> None:
        self._records: dict[str, QualityExecutionRecord] = {}
        self._lifecycles: dict[str, QualityAssessmentLifecycle] = {}
        self._reports: dict[str, QualityReport] = {}
        self._delegate = delegate

    def create_execution(
        self,
        *,
        assessment_id: str,
        dataset_id: DatasetId | str,
        execution_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        started_at: datetime | None = None,
    ) -> QualityExecutionRecord:
        """Create and register a new :class:`QualityExecutionRecord` in PENDING state."""
        exec_id = execution_id or generate_execution_id()
        if not exec_id.strip():
            raise QualityExecutionTrackingError(
                "Execution identifier cannot be empty.",
                assessment_id=assessment_id,
            )
        if exec_id in self._records:
            raise QualityExecutionTrackingError(
                f"Execution '{exec_id}' already exists.",
                assessment_id=assessment_id,
                execution_id=exec_id,
            )

        lifecycle = QualityAssessmentLifecycle(QualityAssessmentState.PENDING)
        timestamp = started_at or datetime.now(UTC)

        record = QualityExecutionRecord(
            id=exec_id,
            assessment_id=assessment_id,
            dataset_id=DatasetId(str(dataset_id)),
            status=ExecutionStatus.PENDING,
            lifecycle_state=lifecycle.state,
            state_history=list(lifecycle.history),
            started_at=timestamp,
            metadata=dict(metadata or {}),
        )
        self._lifecycles[exec_id] = lifecycle
        self._records[exec_id] = record
        return record.model_copy(deep=True)

    def start(self, execution_id: str) -> None:
        """Mark an execution as started (RUNNING) and notify optional delegate."""
        record = self._get_mutable_record(execution_id)
        if record.status in {ExecutionStatus.SUCCEEDED, ExecutionStatus.FAILED}:
            raise QualityExecutionTrackingError(
                f"Cannot start execution '{execution_id}' in terminal status '{record.status}'.",
                assessment_id=record.assessment_id,
                execution_id=execution_id,
            )
        record.status = ExecutionStatus.RUNNING
        if self._delegate is not None:
            try:
                self._delegate.start(execution_id)
            except Exception as exc:
                raise QualityExecutionTrackingError(
                    f"Delegate ExecutionManager failed to start '{execution_id}': {exc}",
                    assessment_id=record.assessment_id,
                    execution_id=execution_id,
                ) from exc

    def record_transition(
        self,
        execution_id: str,
        target_state: QualityAssessmentState,
    ) -> QualityExecutionRecord:
        """Validate and record a lifecycle transition for ``execution_id``."""
        record = self._get_mutable_record(execution_id)
        lifecycle = self._lifecycles[execution_id]
        try:
            lifecycle.transition_to(target_state)
        except Exception as exc:
            raise QualityExecutionTrackingError(
                f"Failed to transition execution '{execution_id}' to '{target_state}': {exc}",
                assessment_id=record.assessment_id,
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
        report: QualityReport | None = None,
        completed_at: datetime | None = None,
    ) -> None:
        """Mark ``execution_id`` as completed and store associated :class:`QualityReport`."""
        record = self._get_mutable_record(execution_id)
        lifecycle = self._lifecycles[execution_id]

        if lifecycle.state != QualityAssessmentState.COMPLETED:
            try:
                lifecycle.transition_to(QualityAssessmentState.COMPLETED)
            except Exception as exc:
                raise QualityExecutionTrackingError(
                    f"Cannot complete execution '{execution_id}' from state "
                    f"'{lifecycle.state}': {exc}",
                    assessment_id=record.assessment_id,
                    execution_id=execution_id,
                ) from exc

        finish_time = completed_at or datetime.now(UTC)
        record.lifecycle_state = lifecycle.state
        record.state_history = list(lifecycle.history)
        record.status = ExecutionStatus.SUCCEEDED
        record.completed_at = finish_time
        record.duration_seconds = max(0.0, (finish_time - record.started_at).total_seconds())

        if report is not None:
            self._reports[report.assessment_id] = report
            record.overall_assessment_status = report.overall_status
            record.total_rules = report.total_rules
            record.passed_rules = report.passed_rules
            record.failed_rules = report.failed_rules
            record.execution_errors = report.execution_errors
            record.quality_score = report.quality_score

        if self._delegate is not None:
            try:
                self._delegate.complete(execution_id)
            except Exception as exc:
                raise QualityExecutionTrackingError(
                    f"Delegate ExecutionManager failed to complete '{execution_id}': {exc}",
                    assessment_id=record.assessment_id,
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

        if lifecycle.state != QualityAssessmentState.FAILED:
            if lifecycle.state == QualityAssessmentState.COMPLETED:
                raise QualityExecutionTrackingError(
                    f"Cannot fail an already completed execution '{execution_id}'.",
                    assessment_id=record.assessment_id,
                    execution_id=execution_id,
                )
            try:
                lifecycle.transition_to(QualityAssessmentState.FAILED)
            except Exception as exc:
                raise QualityExecutionTrackingError(
                    f"Cannot transition execution '{execution_id}' to FAILED: {exc}",
                    assessment_id=record.assessment_id,
                    execution_id=execution_id,
                ) from exc

        finish_time = completed_at or datetime.now(UTC)
        record.lifecycle_state = lifecycle.state
        record.state_history = list(lifecycle.history)
        record.status = ExecutionStatus.FAILED
        record.overall_assessment_status = QualityAssessmentStatus.ERROR
        record.completed_at = finish_time
        record.duration_seconds = max(0.0, (finish_time - record.started_at).total_seconds())
        record.error_message = reason
        record.error_type = error_type or "QualityError"

        if self._delegate is not None:
            try:
                self._delegate.fail(execution_id, reason)
            except Exception as exc:
                raise QualityExecutionTrackingError(
                    "Delegate ExecutionManager failed to record failure for "
                    f"'{execution_id}': {exc}",
                    assessment_id=record.assessment_id,
                    execution_id=execution_id,
                ) from exc

    def store_report(self, report: QualityReport) -> None:
        """Store an immutable :class:`QualityReport` indexed by ``assessment_id``."""
        self._reports[report.assessment_id] = report

    def get_report(self, assessment_id: str) -> QualityReport:
        """Retrieve a stored :class:`QualityReport` by ``assessment_id``."""
        report = self._reports.get(assessment_id)
        if report is None:
            raise QualityExecutionTrackingError(
                f"Quality report for assessment '{assessment_id}' not found.",
                assessment_id=assessment_id,
            )
        return report

    def get_execution(self, execution_id: str) -> QualityExecutionRecord:
        """Return a snapshot of the execution record for ``execution_id``."""
        record = self._get_mutable_record(execution_id)
        return record.model_copy(deep=True)

    def list_executions(
        self,
        *,
        dataset_id: DatasetId | str | None = None,
        status: ExecutionStatus | None = None,
    ) -> list[QualityExecutionRecord]:
        """Return snapshots of tracked execution records with optional filtering."""
        records = [rec.model_copy(deep=True) for rec in self._records.values()]
        if dataset_id is not None:
            target_ds = str(dataset_id)
            records = [r for r in records if str(r.dataset_id) == target_ds]
        if status is not None:
            records = [r for r in records if r.status == status]
        return records

    def _get_mutable_record(self, execution_id: str) -> QualityExecutionRecord:
        record = self._records.get(execution_id)
        if record is None:
            raise QualityExecutionTrackingError(
                f"Execution '{execution_id}' not found in tracker.",
                execution_id=execution_id,
            )
        return record


__all__ = [
    "ExecutionTracker",
    "QualityAssessmentLifecycle",
    "QualityAssessmentState",
    "QualityExecutionRecord",
    "TERMINAL_ASSESSMENT_STATES",
    "VALID_ASSESSMENT_TRANSITIONS",
]
