"""Abstract contract for step execution within atlas-orchestration."""

from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from typing import Any

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
    *,
    timeout_seconds: float | None = None,
    deadline: Any | None = None,
) -> StepExecution:
    """Invoke ``executor.execute`` supporting 2-arg, 3-arg, and timeout-aware executors."""
    sig = inspect.signature(executor.execute)
    params = list(sig.parameters.values())
    param_names = {p.name for p in params}
    accepts_varkw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params)
    accepts_varpos = any(p.kind == inspect.Parameter.VAR_POSITIONAL for p in params)
    positional_params = [
        p
        for p in params
        if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]

    kwargs: dict[str, Any] = {}
    if accepts_varkw or "timeout_seconds" in param_names:
        kwargs["timeout_seconds"] = timeout_seconds
    if accepts_varkw or "deadline" in param_names:
        kwargs["deadline"] = deadline

    if accepts_varpos or len(positional_params) >= 3 or "dependencies" in param_names:
        return executor.execute(step, context, dependencies, **kwargs)
    return executor.execute(step, context, **kwargs)


__all__ = [
    "BaseStepExecutor",
    "invoke_step_executor",
]
