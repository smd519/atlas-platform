"""Abstract contract for step execution within atlas-orchestration."""

from __future__ import annotations

import inspect
from abc import ABC, abstractmethod

from atlas_orchestration.dependencies import OrchestrationDependencies
from atlas_orchestration.execution.context import ExecutionContext
from atlas_orchestration.models.step_execution import StepExecution
from atlas_orchestration.models.workflow_step import WorkflowStep


class BaseStepExecutor(ABC):
    """Abstract base contract for executing a single :class:`WorkflowStep`."""

    @abstractmethod
    def execute(
        self,
        step: WorkflowStep,
        context: ExecutionContext,
        dependencies: OrchestrationDependencies | None = None,
    ) -> StepExecution:
        """Execute ``step`` using the isolated ``context`` and return a :class:`StepExecution`."""


def invoke_step_executor(
    executor: BaseStepExecutor,
    step: WorkflowStep,
    context: ExecutionContext,
    dependencies: OrchestrationDependencies | None = None,
) -> StepExecution:
    """Invoke ``executor.execute`` supporting both 3-arg and 2-arg custom executors."""
    sig = inspect.signature(executor.execute)
    params = list(sig.parameters.values())
    accepts_varargs = any(
        p.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD) for p in params
    )
    if accepts_varargs or len(params) >= 3:
        return executor.execute(step, context, dependencies)
    return executor.execute(step, context)


__all__ = [
    "BaseStepExecutor",
    "invoke_step_executor",
]
