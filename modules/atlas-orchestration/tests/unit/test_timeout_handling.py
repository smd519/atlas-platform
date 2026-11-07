"""Unit tests for cooperative step timeout and monotonic deadline enforcement."""

from __future__ import annotations

import pytest
from atlas_core import ExecutionStatus
from atlas_orchestration.dependencies import OrchestrationDependencies
from atlas_orchestration.exceptions import InvalidStepDefinitionError, StepTimeoutError
from atlas_orchestration.execution.context import ExecutionContext
from atlas_orchestration.execution.executor import BaseStepExecutor
from atlas_orchestration.execution.executors.mock import MockStepExecutor
from atlas_orchestration.execution.timeout import ExecutionDeadline, execute_with_timeout
from atlas_orchestration.models.step_execution import StepExecution
from atlas_orchestration.models.workflow_step import WorkflowStep


class FakeMonotonicClock:
    """Deterministic monotonic clock for testing timeout enforcement."""

    def __init__(self, initial: float = 100.0) -> None:
        self.current = initial

    def __call__(self) -> float:
        return self.current

    def advance(self, seconds: float) -> None:
        self.current += seconds


class DeadlineAwareExecutor(BaseStepExecutor):
    """Executor that inspects the injected cooperative deadline and timeout_seconds."""

    def __init__(self, clock: FakeMonotonicClock, simulated_chunks: list[float]) -> None:
        self.clock = clock
        self.simulated_chunks = simulated_chunks
        self.seen_timeout_seconds: float | None = None

    def execute(
        self,
        step: WorkflowStep,
        context: ExecutionContext,
        dependencies: OrchestrationDependencies | None = None,
        *,
        timeout_seconds: float | None = None,
        deadline: ExecutionDeadline | None = None,
    ) -> StepExecution:
        self.seen_timeout_seconds = timeout_seconds
        for chunk in self.simulated_chunks:
            self.clock.advance(chunk)
            if deadline is not None:
                deadline.check_not_expired(
                    step_id=step.step_id,
                    workflow_id=context.workflow_id,
                    workflow_version=context.workflow_version,
                    execution_id=context.execution_id,
                )
        return StepExecution(
            execution_id=context.execution_id,
            step_id=step.step_id,
            module_target=step.module_target,
            status=ExecutionStatus.SUCCEEDED,
            output_references={"completed_chunks": len(self.simulated_chunks)},
        )


class BuiltinTimeoutExecutor(BaseStepExecutor):
    """Executor that raises standard Python TimeoutError."""

    def execute(
        self,
        step: WorkflowStep,
        context: ExecutionContext,
        dependencies: OrchestrationDependencies | None = None,
    ) -> StepExecution:
        raise TimeoutError("Downstream socket read timed out")


class TestExecutionDeadlineAndTimeout:
    """Tests for ExecutionDeadline and execute_with_timeout."""

    def test_unbounded_deadline_never_expires(self) -> None:
        clock = FakeMonotonicClock(50.0)
        deadline = ExecutionDeadline(timeout_seconds=None, monotonic_clock=clock)
        assert deadline.timeout_seconds is None
        assert deadline.started_at_monotonic == 50.0
        assert deadline.remaining_seconds() is None
        assert deadline.is_expired() is False

        clock.advance(9999.0)
        assert deadline.elapsed_seconds() == pytest.approx(9999.0)
        assert deadline.is_expired() is False
        deadline.check_not_expired(step_id="s1")

    def test_bounded_deadline_tracks_remaining_and_expiration(self) -> None:
        clock = FakeMonotonicClock(10.0)
        deadline = ExecutionDeadline(timeout_seconds=5.0, monotonic_clock=clock)
        assert deadline.timeout_seconds == 5.0
        assert deadline.remaining_seconds() == pytest.approx(5.0)
        assert deadline.is_expired() is False

        clock.advance(3.0)
        assert deadline.elapsed_seconds() == pytest.approx(3.0)
        assert deadline.remaining_seconds() == pytest.approx(2.0)
        assert deadline.is_expired() is False
        deadline.check_not_expired(step_id="s1")

        clock.advance(2.5)
        assert deadline.elapsed_seconds() == pytest.approx(5.5)
        assert deadline.remaining_seconds() == pytest.approx(0.0)
        assert deadline.is_expired() is True

        with pytest.raises(StepTimeoutError) as exc_info:
            deadline.check_not_expired(
                step_id="s1",
                workflow_id="wf_1",
                workflow_version=1,
                execution_id="exec_1",
                step_execution_id="step_exec_1",
            )
        assert exc_info.value.timeout_seconds == 5.0
        assert exc_info.value.step_id == "s1"
        assert exc_info.value.execution_id == "exec_1"

    def test_invalid_deadline_timeout_rejected(self) -> None:
        with pytest.raises(InvalidStepDefinitionError):
            ExecutionDeadline(timeout_seconds=0.0)
        with pytest.raises(InvalidStepDefinitionError):
            ExecutionDeadline(timeout_seconds=-1.0)
        with pytest.raises(InvalidStepDefinitionError):
            ExecutionDeadline(timeout_seconds=True)

    def test_step_completes_within_timeout(self) -> None:
        clock = FakeMonotonicClock(0.0)
        executor = MockStepExecutor(
            step_outputs={"s1": {"rows": 42}},
            step_durations={"s1": 1.5},
            clock_advance_fn=clock.advance,
        )
        step = WorkflowStep(
            step_id="s1",
            module_target="ingestion",
            action="ingest",
            timeout_seconds=3.0,
        )
        ctx = ExecutionContext(workflow_id="wf_1", workflow_version=1, execution_id="exec_1")

        result, elapsed = execute_with_timeout(
            executor,
            step,
            ctx,
            monotonic_clock=clock,
            step_execution_id="step_exec_1",
        )
        assert result.status == ExecutionStatus.SUCCEEDED
        assert result.output_references == {"rows": 42}
        assert elapsed == pytest.approx(1.5)

    def test_step_exceeding_timeout_raises_step_timeout_error_at_boundary(self) -> None:
        clock = FakeMonotonicClock(0.0)
        executor = MockStepExecutor(
            step_outputs={"s1": {"rows": 42}},
            step_durations={"s1": 4.0},
            clock_advance_fn=clock.advance,
        )
        step = WorkflowStep(
            step_id="s1",
            module_target="ingestion",
            action="ingest",
            timeout_seconds=2.5,
        )
        ctx = ExecutionContext(workflow_id="wf_1", workflow_version=1, execution_id="exec_1")

        with pytest.raises(StepTimeoutError) as exc_info:
            execute_with_timeout(
                executor,
                step,
                ctx,
                monotonic_clock=clock,
                step_execution_id="step_exec_1",
            )
        assert exc_info.value.timeout_seconds == 2.5
        assert exc_info.value.step_id == "s1"
        assert ctx.step_outputs == {}

    def test_cooperative_deadline_aware_executor_aborts_mid_step(self) -> None:
        clock = FakeMonotonicClock(0.0)
        executor = DeadlineAwareExecutor(clock, simulated_chunks=[1.0, 1.5, 1.0])
        step = WorkflowStep(
            step_id="s_coop",
            module_target="processing",
            action="process",
            timeout_seconds=2.0,
        )
        ctx = ExecutionContext(workflow_id="wf_1", workflow_version=1, execution_id="exec_1")

        with pytest.raises(StepTimeoutError) as exc_info:
            execute_with_timeout(
                executor,
                step,
                ctx,
                monotonic_clock=clock,
                step_execution_id="step_exec_coop",
            )
        assert executor.seen_timeout_seconds == 2.0
        assert exc_info.value.timeout_seconds == 2.0
        # Aborted after second chunk (1.0 + 1.5 = 2.5s > 2.0s), third chunk never ran
        assert clock() == pytest.approx(2.5)

    def test_builtin_timeout_error_wrapped_as_step_timeout_error(self) -> None:
        clock = FakeMonotonicClock(0.0)
        executor = BuiltinTimeoutExecutor()
        step = WorkflowStep(
            step_id="s_sock",
            module_target="ingestion",
            action="ingest",
            timeout_seconds=5.0,
        )
        ctx = ExecutionContext(workflow_id="wf_1", workflow_version=1, execution_id="exec_1")

        with pytest.raises(StepTimeoutError) as exc_info:
            execute_with_timeout(
                executor,
                step,
                ctx,
                monotonic_clock=clock,
                step_execution_id="step_exec_sock",
            )
        assert exc_info.value.timeout_seconds == 5.0
        assert exc_info.value.step_id == "s_sock"
        assert "Downstream socket read timed out" in str(exc_info.value)
