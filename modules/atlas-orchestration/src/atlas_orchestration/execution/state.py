"""Execution state management contract for atlas-orchestration."""

from __future__ import annotations

from abc import ABC, abstractmethod

from atlas_core import ExecutionManager, ExecutionStatus

from atlas_orchestration.models.step_execution import StepExecution
from atlas_orchestration.models.workflow_execution import WorkflowExecution


class StateManager(ExecutionManager, ABC):
    """Abstract state management contract for workflow and step executions.

    Extends :class:`atlas_core.ExecutionManager` so workflow state management
    integrates seamlessly with platform-wide execution lifecycle contracts.
    """

    @abstractmethod
    def create_workflow_execution(self, execution: WorkflowExecution) -> WorkflowExecution:
        """Persist a new :class:`WorkflowExecution` state record."""

    @abstractmethod
    def get_workflow_execution(self, execution_id: str) -> WorkflowExecution:
        """Retrieve a :class:`WorkflowExecution` by ``execution_id``."""

    @abstractmethod
    def update_workflow_execution(self, execution: WorkflowExecution) -> WorkflowExecution:
        """Update an existing :class:`WorkflowExecution` state record."""

    @abstractmethod
    def list_workflow_executions(
        self,
        workflow_id: str | None = None,
        status: ExecutionStatus | None = None,
    ) -> list[WorkflowExecution]:
        """List workflow executions optionally filtered by ``workflow_id`` and ``status``."""

    @abstractmethod
    def create_step_execution(self, step_execution: StepExecution) -> StepExecution:
        """Persist a new :class:`StepExecution` state record."""

    @abstractmethod
    def get_step_execution(self, step_execution_id: str) -> StepExecution:
        """Retrieve a :class:`StepExecution` by ``step_execution_id``."""

    @abstractmethod
    def update_step_execution(self, step_execution: StepExecution) -> StepExecution:
        """Update an existing :class:`StepExecution` state record."""

    @abstractmethod
    def list_step_executions(self, execution_id: str) -> list[StepExecution]:
        """List all step executions associated with ``execution_id``."""

    @abstractmethod
    def start(self, execution_id: str) -> None:
        """Transition ``execution_id`` to ``RUNNING`` and set start timestamp."""

    @abstractmethod
    def complete(self, execution_id: str) -> None:
        """Transition ``execution_id`` to ``SUCCEEDED`` and set completion timestamp."""

    @abstractmethod
    def fail(self, execution_id: str, reason: str) -> None:
        """Transition ``execution_id`` to ``FAILED`` with ``reason`` and completion timestamp."""


__all__ = ["StateManager"]
