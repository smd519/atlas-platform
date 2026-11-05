"""Mock step executor for testing sequential workflow execution in isolation."""

from __future__ import annotations

import copy
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from atlas_core import ExecutionStatus

from atlas_orchestration.dependencies import OrchestrationDependencies
from atlas_orchestration.exceptions import StepExecutionError
from atlas_orchestration.execution.context import ExecutionContext
from atlas_orchestration.execution.executor import BaseStepExecutor
from atlas_orchestration.models.step_execution import StepExecution
from atlas_orchestration.models.workflow_step import WorkflowStep


class MockStepExecutor(BaseStepExecutor):
    """Configurable mock :class:`BaseStepExecutor` for unit and lifecycle testing."""

    def __init__(
        self,
        *,
        step_outputs: dict[str, dict[str, Any]] | None = None,
        failing_steps: dict[str, Exception | str] | set[str] | None = None,
        handler: Callable[[WorkflowStep, ExecutionContext], dict[str, Any]] | None = None,
    ) -> None:
        self._step_outputs: dict[str, dict[str, Any]] = copy.deepcopy(step_outputs or {})
        self._failing_steps = failing_steps or {}
        self._handler = handler
        self.executed_steps: list[WorkflowStep] = []

    @property
    def executed_step_ids(self) -> list[str]:
        """Return the ordered list of ``step_id`` values executed so far."""
        return [step.step_id for step in self.executed_steps]

    def set_step_output(self, step_id: str, output: dict[str, Any]) -> None:
        """Configure the mock output dictionary returned when ``step_id`` runs."""
        self._step_outputs[step_id] = copy.deepcopy(output)

    def execute(
        self,
        step: WorkflowStep,
        context: ExecutionContext,
        dependencies: OrchestrationDependencies | None = None,
    ) -> StepExecution:
        """Record execution of ``step``, raise if configured to fail, or return mock outputs."""
        start_ts = datetime.now(UTC)
        self.executed_steps.append(step.model_copy(deep=True))

        if isinstance(self._failing_steps, set) and step.step_id in self._failing_steps:
            raise StepExecutionError(
                f"Configured mock failure for step '{step.step_id}'.",
                workflow_id=context.workflow_id,
                workflow_version=context.workflow_version,
                execution_id=context.execution_id,
                step_id=step.step_id,
            )
        if isinstance(self._failing_steps, dict) and step.step_id in self._failing_steps:
            err = self._failing_steps[step.step_id]
            if isinstance(err, Exception):
                raise err
            raise StepExecutionError(
                str(err),
                workflow_id=context.workflow_id,
                workflow_version=context.workflow_version,
                execution_id=context.execution_id,
                step_id=step.step_id,
            )

        if self._handler is not None:
            outputs = copy.deepcopy(self._handler(step, context))
        elif step.step_id in self._step_outputs:
            outputs = copy.deepcopy(self._step_outputs[step.step_id])
        else:
            outputs = {
                "step_id": step.step_id,
                "action": step.action,
                "resolved_parameters": copy.deepcopy(step.parameters),
            }

        end_ts = datetime.now(UTC)
        return StepExecution(
            execution_id=context.execution_id,
            step_id=step.step_id,
            module_target=step.module_target,
            status=ExecutionStatus.SUCCEEDED,
            start_time=start_ts,
            end_time=max(end_ts, start_ts),
            module_execution_ref=f"mock_{context.execution_id}_{step.step_id}",
            output_references=outputs,
        )


__all__ = ["MockStepExecutor"]

