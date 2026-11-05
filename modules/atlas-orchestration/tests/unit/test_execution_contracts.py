"""Unit tests for BaseStepExecutor, StateManager, and ExecutionTracker contracts."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from atlas_core import ExecutionManager, ExecutionStatus
from atlas_orchestration import (
    BaseStepExecutor,
    ExecutionContext,
    ExecutionTracker,
    InMemoryExecutionTracker,
    StateManager,
    StepExecution,
    WorkflowStep,
)


class _DummyStepExecutor(BaseStepExecutor):
    """Concrete test implementation of BaseStepExecutor."""

    def execute(self, step: WorkflowStep, context: ExecutionContext) -> StepExecution:
        now = datetime.now(UTC)
        output = {"executed_action": step.action, "target": step.module_target}
        context.set_step_output(step.step_id, output)
        return StepExecution(
            execution_id=context.execution_id,
            step_id=step.step_id,
            module_target=step.module_target,
            status=ExecutionStatus.SUCCEEDED,
            start_time=now,
            end_time=now,
            output_references=output,
        )


def test_base_step_executor_is_abstract_and_subclassable() -> None:
    with pytest.raises(TypeError):
        BaseStepExecutor()  # type: ignore[abstract]

    executor = _DummyStepExecutor()
    ctx = ExecutionContext(workflow_id="wf_contract", execution_id="exec_c1")
    step = WorkflowStep(step_id="step_1", module_target="atlas-ingestion", action="ingest")

    result = executor.execute(step, ctx)
    assert result.status == ExecutionStatus.SUCCEEDED
    assert result.step_id == "step_1"
    assert ctx.get_step_output("step_1") == {
        "executed_action": "ingest",
        "target": "atlas-ingestion",
    }


def test_state_manager_and_execution_tracker_hierarchy() -> None:
    with pytest.raises(TypeError):
        StateManager()  # type: ignore[abstract]

    with pytest.raises(TypeError):
        ExecutionTracker()  # type: ignore[abstract]

    tracker = InMemoryExecutionTracker()
    assert isinstance(tracker, ExecutionTracker)
    assert isinstance(tracker, StateManager)
    assert isinstance(tracker, ExecutionManager)
