"""Thread-safe in-memory execution tracker implementation."""

from __future__ import annotations

import threading
from datetime import UTC, datetime

from atlas_core import ExecutionStatus

from atlas_orchestration.exceptions import (
    InvalidWorkflowStateError,
    StepExecutionNotFoundError,
    WorkflowExecutionNotFoundError,
)
from atlas_orchestration.models.step_execution import StepExecution
from atlas_orchestration.models.workflow_execution import WorkflowExecution
from atlas_orchestration.tracking.execution_tracker import ExecutionTracker


class InMemoryExecutionTracker(ExecutionTracker):
    """Thread-safe in-memory implementation of :class:`ExecutionTracker`."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._workflow_executions: dict[str, WorkflowExecution] = {}
        self._step_executions: dict[str, StepExecution] = {}
        self._execution_step_ids: dict[str, list[str]] = {}

    def create_workflow_execution(self, execution: WorkflowExecution) -> WorkflowExecution:
        """Persist a new :class:`WorkflowExecution` record atomically."""
        if not isinstance(execution, WorkflowExecution):
            raise InvalidWorkflowStateError(
                f"Expected WorkflowExecution, got {type(execution).__name__}."
            )
        snapshot = execution.model_copy(deep=True)

        with self._lock:
            if snapshot.execution_id in self._workflow_executions:
                raise InvalidWorkflowStateError(
                    f"Workflow execution '{snapshot.execution_id}' already exists.",
                    workflow_id=snapshot.workflow_id,
                    workflow_version=snapshot.workflow_version,
                    execution_id=snapshot.execution_id,
                )

            self._workflow_executions[snapshot.execution_id] = snapshot
            self._execution_step_ids[snapshot.execution_id] = []
            return snapshot.model_copy(deep=True)

    def get_workflow_execution(self, execution_id: str) -> WorkflowExecution:
        """Retrieve an isolated snapshot of a :class:`WorkflowExecution` by ID."""
        clean_id = execution_id.strip() if isinstance(execution_id, str) else ""
        if not clean_id:
            raise WorkflowExecutionNotFoundError("Execution identifier must not be empty.")

        with self._lock:
            execution = self._workflow_executions.get(clean_id)
            if execution is None:
                raise WorkflowExecutionNotFoundError(
                    f"Workflow execution '{clean_id}' not found.",
                    execution_id=clean_id,
                )
            return execution.model_copy(deep=True)

    def update_workflow_execution(self, execution: WorkflowExecution) -> WorkflowExecution:
        """Update an existing :class:`WorkflowExecution` record atomically."""
        if not isinstance(execution, WorkflowExecution):
            raise InvalidWorkflowStateError(
                f"Expected WorkflowExecution, got {type(execution).__name__}."
            )
        snapshot = execution.model_copy(deep=True)

        with self._lock:
            existing = self._workflow_executions.get(snapshot.execution_id)
            if existing is None:
                raise WorkflowExecutionNotFoundError(
                    f"Workflow execution '{snapshot.execution_id}' not found.",
                    workflow_id=snapshot.workflow_id,
                    workflow_version=snapshot.workflow_version,
                    execution_id=snapshot.execution_id,
                )
            if (
                existing.workflow_id != snapshot.workflow_id
                or existing.workflow_version != snapshot.workflow_version
            ):
                raise InvalidWorkflowStateError(
                    "Cannot change workflow_id or workflow_version of an existing execution.",
                    workflow_id=snapshot.workflow_id,
                    workflow_version=snapshot.workflow_version,
                    execution_id=snapshot.execution_id,
                )

            self._workflow_executions[snapshot.execution_id] = snapshot
            return snapshot.model_copy(deep=True)

    def list_workflow_executions(
        self,
        workflow_id: str | None = None,
        status: ExecutionStatus | None = None,
    ) -> list[WorkflowExecution]:
        """List workflow executions optionally filtered by ``workflow_id`` and ``status``."""
        clean_wf_id = workflow_id.strip() if isinstance(workflow_id, str) else None
        with self._lock:
            results: list[WorkflowExecution] = []
            for execution in self._workflow_executions.values():
                if clean_wf_id is not None and execution.workflow_id != clean_wf_id:
                    continue
                if status is not None and execution.status != status:
                    continue
                results.append(execution.model_copy(deep=True))
            return results

    def create_step_execution(self, step_execution: StepExecution) -> StepExecution:
        """Persist a new :class:`StepExecution` record linked to an existing workflow execution."""
        if not isinstance(step_execution, StepExecution):
            raise InvalidWorkflowStateError(
                f"Expected StepExecution, got {type(step_execution).__name__}."
            )
        snapshot = step_execution.model_copy(deep=True)

        with self._lock:
            parent = self._workflow_executions.get(snapshot.execution_id)
            if parent is None:
                raise WorkflowExecutionNotFoundError(
                    f"Parent workflow execution '{snapshot.execution_id}' not found "
                    f"for step '{snapshot.step_id}'.",
                    execution_id=snapshot.execution_id,
                    step_id=snapshot.step_id,
                    step_execution_id=snapshot.step_execution_id,
                )
            if snapshot.step_execution_id in self._step_executions:
                raise InvalidWorkflowStateError(
                    f"Step execution '{snapshot.step_execution_id}' already exists.",
                    workflow_id=parent.workflow_id,
                    workflow_version=parent.workflow_version,
                    execution_id=snapshot.execution_id,
                    step_id=snapshot.step_id,
                    step_execution_id=snapshot.step_execution_id,
                )

            self._step_executions[snapshot.step_execution_id] = snapshot
            self._execution_step_ids[snapshot.execution_id].append(snapshot.step_execution_id)
            return snapshot.model_copy(deep=True)

    def get_step_execution(self, step_execution_id: str) -> StepExecution:
        """Retrieve an isolated snapshot of a :class:`StepExecution` by ID."""
        clean_id = step_execution_id.strip() if isinstance(step_execution_id, str) else ""
        if not clean_id:
            raise StepExecutionNotFoundError("Step execution identifier must not be empty.")

        with self._lock:
            step = self._step_executions.get(clean_id)
            if step is None:
                raise StepExecutionNotFoundError(
                    f"Step execution '{clean_id}' not found.",
                    step_execution_id=clean_id,
                )
            return step.model_copy(deep=True)

    def update_step_execution(self, step_execution: StepExecution) -> StepExecution:
        """Update an existing :class:`StepExecution` record atomically."""
        if not isinstance(step_execution, StepExecution):
            raise InvalidWorkflowStateError(
                f"Expected StepExecution, got {type(step_execution).__name__}."
            )
        snapshot = step_execution.model_copy(deep=True)

        with self._lock:
            existing = self._step_executions.get(snapshot.step_execution_id)
            if existing is None:
                raise StepExecutionNotFoundError(
                    f"Step execution '{snapshot.step_execution_id}' not found.",
                    execution_id=snapshot.execution_id,
                    step_id=snapshot.step_id,
                    step_execution_id=snapshot.step_execution_id,
                )
            if (
                existing.execution_id != snapshot.execution_id
                or existing.step_id != snapshot.step_id
            ):
                raise InvalidWorkflowStateError(
                    "Cannot change execution_id or step_id of an existing step execution.",
                    execution_id=snapshot.execution_id,
                    step_id=snapshot.step_id,
                    step_execution_id=snapshot.step_execution_id,
                )

            self._step_executions[snapshot.step_execution_id] = snapshot
            return snapshot.model_copy(deep=True)

    def list_step_executions(self, execution_id: str) -> list[StepExecution]:
        """Return all :class:`StepExecution` records for ``execution_id`` in creation order."""
        clean_id = execution_id.strip() if isinstance(execution_id, str) else ""
        if not clean_id:
            raise WorkflowExecutionNotFoundError("Execution identifier must not be empty.")

        with self._lock:
            if clean_id not in self._workflow_executions:
                raise WorkflowExecutionNotFoundError(
                    f"Workflow execution '{clean_id}' not found.",
                    execution_id=clean_id,
                )
            step_ids = self._execution_step_ids.get(clean_id, [])
            return [self._step_executions[s_id].model_copy(deep=True) for s_id in step_ids]

    def start(self, execution_id: str) -> None:
        """Transition ``execution_id`` to ``RUNNING`` status with a start timestamp."""
        with self._lock:
            current = self.get_workflow_execution(execution_id)
            now = datetime.now(UTC)
            start_ts = current.start_time or now
            updated = WorkflowExecution(
                execution_id=current.execution_id,
                workflow_id=current.workflow_id,
                workflow_version=current.workflow_version,
                status=ExecutionStatus.RUNNING,
                start_time=start_ts,
                end_time=current.end_time,
                context_overrides=current.context_overrides,
                error_message=current.error_message,
            )
            self.update_workflow_execution(updated)

    def complete(self, execution_id: str) -> None:
        """Transition ``execution_id`` to ``SUCCEEDED`` status with a completion timestamp."""
        with self._lock:
            current = self.get_workflow_execution(execution_id)
            now = datetime.now(UTC)
            start_ts = current.start_time or now
            end_ts = max(now, start_ts)
            updated = WorkflowExecution(
                execution_id=current.execution_id,
                workflow_id=current.workflow_id,
                workflow_version=current.workflow_version,
                status=ExecutionStatus.SUCCEEDED,
                start_time=start_ts,
                end_time=end_ts,
                context_overrides=current.context_overrides,
                error_message=current.error_message,
            )
            self.update_workflow_execution(updated)

    def fail(self, execution_id: str, reason: str) -> None:
        """Transition ``execution_id`` to ``FAILED`` status with ``reason`` and end timestamp."""
        with self._lock:
            current = self.get_workflow_execution(execution_id)
            now = datetime.now(UTC)
            start_ts = current.start_time or now
            end_ts = max(now, start_ts)
            updated = WorkflowExecution(
                execution_id=current.execution_id,
                workflow_id=current.workflow_id,
                workflow_version=current.workflow_version,
                status=ExecutionStatus.FAILED,
                start_time=start_ts,
                end_time=end_ts,
                context_overrides=current.context_overrides,
                error_message=reason,
            )
            self.update_workflow_execution(updated)


__all__ = ["InMemoryExecutionTracker"]
