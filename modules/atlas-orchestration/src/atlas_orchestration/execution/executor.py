"""Abstract contract for step execution within atlas-orchestration."""

from __future__ import annotations

from abc import ABC, abstractmethod

from atlas_orchestration.execution.context import ExecutionContext
from atlas_orchestration.models.step_execution import StepExecution
from atlas_orchestration.models.workflow_step import WorkflowStep


class BaseStepExecutor(ABC):
    """Abstract base contract for executing a single :class:`WorkflowStep`."""

    @abstractmethod
    def execute(self, step: WorkflowStep, context: ExecutionContext) -> StepExecution:
        """Execute ``step`` using the isolated ``context`` and return a :class:`StepExecution`."""


__all__ = ["BaseStepExecutor"]

