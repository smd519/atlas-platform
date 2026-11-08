"""Cooperative timeout and monotonic deadline enforcement for step execution."""

from __future__ import annotations

import math
import time
from collections.abc import Callable

from atlas_orchestration.dependencies import OrchestrationDependencies
from atlas_orchestration.exceptions import InvalidStepDefinitionError, StepTimeoutError
from atlas_orchestration.execution.context import ExecutionContext
from atlas_orchestration.execution.executor import BaseStepExecutor, invoke_step_executor
from atlas_orchestration.models.step_execution import StepExecution
from atlas_orchestration.models.workflow_step import WorkflowStep


class ExecutionDeadline:
    """Monotonic clock deadline tracker for cooperative step timeout enforcement.

    Does not spawn background threads; instead tracks elapsed monotonic time
    and allows both ``WorkflowEngine`` and timeout-aware executors to verify
    whether a step's execution budget has expired.
    """

    def __init__(
        self,
        *,
        timeout_seconds: float | None,
        started_at_monotonic: float | None = None,
        monotonic_clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if timeout_seconds is not None and (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or math.isnan(float(timeout_seconds))
            or math.isinf(float(timeout_seconds))
            or float(timeout_seconds) <= 0.0
        ):
            raise InvalidStepDefinitionError(
                f"timeout_seconds must be a positive finite number, got {timeout_seconds!r}."
            )
        self._timeout_seconds = float(timeout_seconds) if timeout_seconds is not None else None
        self._monotonic_clock = monotonic_clock
        self._started_at_monotonic = (
            started_at_monotonic if started_at_monotonic is not None else monotonic_clock()
        )

    @property
    def timeout_seconds(self) -> float | None:
        """Configured timeout threshold in seconds, or ``None`` if unbounded."""
        return self._timeout_seconds

    @property
    def started_at_monotonic(self) -> float:
        """Monotonic timestamp when the attempt began."""
        return self._started_at_monotonic

    def elapsed_seconds(self) -> float:
        """Elapsed monotonic time in seconds since ``started_at_monotonic``."""
        return max(0.0, float(self._monotonic_clock() - self._started_at_monotonic))

    def remaining_seconds(self) -> float | None:
        """Remaining time budget in seconds, or ``None`` if no timeout is configured."""
        if self._timeout_seconds is None:
            return None
        return max(0.0, self._timeout_seconds - self.elapsed_seconds())

    def is_expired(self) -> bool:
        """Return ``True`` if elapsed monotonic time strictly exceeds ``timeout_seconds``."""
        if self._timeout_seconds is None:
            return False
        return self.elapsed_seconds() > self._timeout_seconds

    def check_not_expired(
        self,
        *,
        step_id: str | None = None,
        workflow_id: str | None = None,
        workflow_version: int | None = None,
        execution_id: str | None = None,
        step_execution_id: str | None = None,
    ) -> None:
        """Raise :class:`StepTimeoutError` if the deadline has expired."""
        if self._timeout_seconds is not None:
            elapsed = self.elapsed_seconds()
            if elapsed > self._timeout_seconds:
                raise StepTimeoutError(
                    f"Step '{step_id or '<unknown>'}' exceeded timeout of "
                    f"{self._timeout_seconds:.3f}s (elapsed {elapsed:.3f}s).",
                    timeout_seconds=self._timeout_seconds,
                    workflow_id=workflow_id,
                    workflow_version=workflow_version,
                    execution_id=execution_id,
                    step_id=step_id,
                    step_execution_id=step_execution_id,
                )


def execute_with_timeout(
    executor: BaseStepExecutor,
    step: WorkflowStep,
    context: ExecutionContext,
    dependencies: OrchestrationDependencies | None = None,
    *,
    monotonic_clock: Callable[[], float] = time.monotonic,
    step_execution_id: str | None = None,
) -> tuple[StepExecution, float]:
    """Execute ``step`` with cooperative monotonic timeout enforcement.

    Returns a tuple of ``(step_execution_result, elapsed_seconds)``.
    Raises :class:`StepTimeoutError` if the executor raises ``StepTimeoutError``
    or ``TimeoutError``, or if the elapsed monotonic duration exceeds
    ``step.timeout_seconds``.
    """
    deadline = ExecutionDeadline(
        timeout_seconds=step.timeout_seconds,
        monotonic_clock=monotonic_clock,
    )
    try:
        result = invoke_step_executor(
            executor,
            step,
            context,
            dependencies,
            timeout_seconds=step.timeout_seconds,
            deadline=deadline,
        )
    except StepTimeoutError as exc:
        raise StepTimeoutError(
            str(exc),
            timeout_seconds=exc.timeout_seconds or step.timeout_seconds,
            retryable=exc.retryable,
            workflow_id=exc.workflow_id or context.workflow_id,
            workflow_version=exc.workflow_version or context.workflow_version,
            execution_id=exc.execution_id or context.execution_id,
            step_id=exc.step_id or step.step_id,
            step_execution_id=exc.step_execution_id or step_execution_id,
        ) from exc
    except TimeoutError as exc:
        raise StepTimeoutError(
            f"Step '{step.step_id}' timed out: {exc}",
            timeout_seconds=step.timeout_seconds,
            workflow_id=context.workflow_id,
            workflow_version=context.workflow_version,
            execution_id=context.execution_id,
            step_id=step.step_id,
            step_execution_id=step_execution_id,
        ) from exc

    deadline.check_not_expired(
        step_id=step.step_id,
        workflow_id=context.workflow_id,
        workflow_version=context.workflow_version,
        execution_id=context.execution_id,
        step_execution_id=step_execution_id,
    )
    return result, deadline.elapsed_seconds()


__all__ = [
    "ExecutionDeadline",
    "execute_with_timeout",
]
