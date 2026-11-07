"""Sequential workflow execution engine for atlas-orchestration."""

from __future__ import annotations

import copy
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from atlas_core import ExecutionStatus

from atlas_orchestration.dependencies import OrchestrationDependencies
from atlas_orchestration.exceptions import StepExecutionError, StepTimeoutError
from atlas_orchestration.execution.context import ExecutionContext
from atlas_orchestration.execution.registry import StepExecutorRegistry
from atlas_orchestration.execution.resolver import ContextResolver
from atlas_orchestration.execution.timeout import execute_with_timeout
from atlas_orchestration.models.events import (
    EventSubscriber,
    ExecutionEventPublisher,
    StepCancelled,
    StepFailed,
    StepRetryScheduled,
    StepStarted,
    StepSucceeded,
    StepTimedOut,
    WorkflowFailed,
    WorkflowStarted,
    WorkflowSucceeded,
)
from atlas_orchestration.models.retry import RetryPolicy, is_retryable_exception
from atlas_orchestration.models.step_execution import StepAttempt, StepExecution
from atlas_orchestration.models.workflow import WorkflowDefinition
from atlas_orchestration.models.workflow_execution import WorkflowExecution
from atlas_orchestration.models.workflow_result import WorkflowResult
from atlas_orchestration.tracking.execution_tracker import ExecutionTracker
from atlas_orchestration.tracking.memory import InMemoryExecutionTracker
from atlas_orchestration.validation.workflow_validator import WorkflowValidator


class WorkflowEngine:
    """Sequential workflow execution engine with retry, timeout, and observability support.

    Coordinates workflow validation, topological ordering, execution context
    initialization, dynamic parameter resolution, step execution with configurable
    retries and cooperative timeouts, structured event emission, fail-fast
    cancellation of downstream steps, and state persistence.
    """

    def __init__(
        self,
        executor_registry: StepExecutorRegistry,
        *,
        validator: WorkflowValidator | None = None,
        resolver: ContextResolver | None = None,
        tracker: ExecutionTracker | None = None,
        dependencies: OrchestrationDependencies | None = None,
        event_publisher: ExecutionEventPublisher | None = None,
        sleep_fn: Callable[[float], None] = time.sleep,
        monotonic_clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._executor_registry = executor_registry
        self._validator = validator or WorkflowValidator()
        self._resolver = resolver or ContextResolver()
        self._tracker: ExecutionTracker = tracker or InMemoryExecutionTracker()
        self._dependencies = dependencies or OrchestrationDependencies()
        self._event_publisher = event_publisher or ExecutionEventPublisher()
        self._sleep_fn = sleep_fn
        self._monotonic_clock = monotonic_clock

    @property
    def executor_registry(self) -> StepExecutorRegistry:
        """Return the :class:`StepExecutorRegistry` used by this engine."""
        return self._executor_registry

    @property
    def validator(self) -> WorkflowValidator:
        """Return the :class:`WorkflowValidator` used by this engine."""
        return self._validator

    @property
    def resolver(self) -> ContextResolver:
        """Return the :class:`ContextResolver` used by this engine."""
        return self._resolver

    @property
    def tracker(self) -> ExecutionTracker:
        """Return the :class:`ExecutionTracker` used by this engine."""
        return self._tracker

    @property
    def dependencies(self) -> OrchestrationDependencies:
        """Return the :class:`OrchestrationDependencies` container used by this engine."""
        return self._dependencies

    @property
    def event_publisher(self) -> ExecutionEventPublisher:
        """Return the :class:`ExecutionEventPublisher` used by this engine."""
        return self._event_publisher

    def register_event_listener(self, listener: EventSubscriber) -> None:
        """Register an event listener on this engine's :class:`ExecutionEventPublisher`."""
        self._event_publisher.register_listener(listener)

    def unregister_event_listener(self, listener: EventSubscriber) -> None:
        """Unregister an event listener from this engine's :class:`ExecutionEventPublisher`."""
        self._event_publisher.unregister_listener(listener)

    def execute(
        self,
        workflow: WorkflowDefinition,
        *,
        parameters: dict[str, Any] | None = None,
        execution_id: str | None = None,
        correlation_id: str | None = None,
        context: ExecutionContext | None = None,
        raise_on_error: bool = False,
    ) -> WorkflowResult:
        """Validate and sequentially execute ``workflow`` in topological order.

        Parameters
        ----------
        workflow:
            The :class:`WorkflowDefinition` to execute.
        parameters:
            Optional workflow-level input variables stored in ``context.workflow_variables``.
        execution_id:
            Optional explicit execution ID (auto-generated if omitted).
        correlation_id:
            Optional correlation ID propagated into ``context.execution_metadata``.
        context:
            Optional caller-supplied :class:`ExecutionContext` to populate and inspect.
        raise_on_error:
            When ``True``, re-raises the step or resolution exception after recording
            ``FAILED`` and ``CANCELLED`` states in the tracker. When ``False`` (default),
            returns a failed :class:`WorkflowResult`.
        """
        validation = self._validator.validate(workflow)
        topological_order = validation.topological_order

        if context is not None:
            exec_context = context
            if parameters:
                for key, value in parameters.items():
                    exec_context.set_variable(key, value)
        else:
            exec_context = ExecutionContext(
                workflow_id=workflow.workflow_id,
                workflow_version=workflow.version,
                execution_id=execution_id,
                workflow_variables=copy.deepcopy(parameters) if parameters else None,
                correlation_id=correlation_id,
            )

        exec_id = exec_context.execution_id

        # 1. Create WorkflowExecution in PENDING state, then transition to RUNNING
        initial_wf_exec = WorkflowExecution(
            execution_id=exec_id,
            workflow_id=workflow.workflow_id,
            workflow_version=workflow.version,
            status=ExecutionStatus.PENDING,
            context_overrides=copy.deepcopy(parameters) if parameters else {},
        )
        self._tracker.create_workflow_execution(initial_wf_exec)
        self._tracker.start(exec_id)
        running_wf_exec = self._tracker.get_workflow_execution(exec_id)
        wf_start_time = running_wf_exec.start_time or datetime.now(UTC)
        wf_start_mono = self._monotonic_clock()

        self._event_publisher.emit(
            WorkflowStarted(
                execution_id=exec_id,
                workflow_id=workflow.workflow_id,
                workflow_version=workflow.version,
                timestamp=wf_start_time,
            )
        )

        # 2. Initialize all StepExecution records in PENDING state in topological order
        step_records: dict[str, StepExecution] = {}
        for step_id in topological_order:
            step_def = workflow.get_step(step_id)
            assert step_def is not None
            pending_step = StepExecution(
                execution_id=exec_id,
                step_id=step_def.step_id,
                module_target=step_def.module_target,
                status=ExecutionStatus.PENDING,
                attempt_number=1,
            )
            created_step = self._tracker.create_step_execution(pending_step)
            step_records[step_id] = created_step

        # 3. Sequentially execute steps in topological order
        for idx, step_id in enumerate(topological_order):
            step_def = workflow.get_step(step_id)
            assert step_def is not None
            current_record = step_records[step_id]
            step_first_start_ts = datetime.now(UTC)
            step_first_start_mono = self._monotonic_clock()
            retry_policy = step_def.retry_policy or RetryPolicy(max_attempts=1)
            max_attempts = retry_policy.max_attempts

            running_record = current_record.model_copy(
                update={
                    "status": ExecutionStatus.RUNNING,
                    "start_time": step_first_start_ts,
                    "attempt_number": 1,
                },
                deep=True,
            )
            step_records[step_id] = self._tracker.update_step_execution(running_record)

            for attempt_num in range(1, max_attempts + 1):
                attempt_start_ts = datetime.now(UTC)
                if attempt_num > 1:
                    latest_running = self._tracker.get_step_execution(
                        current_record.step_execution_id
                    )
                    step_records[step_id] = self._tracker.update_step_execution(
                        latest_running.model_copy(
                            update={"attempt_number": attempt_num},
                            deep=True,
                        )
                    )

                self._event_publisher.emit(
                    StepStarted(
                        execution_id=exec_id,
                        workflow_id=workflow.workflow_id,
                        workflow_version=workflow.version,
                        step_id=step_id,
                        step_execution_id=current_record.step_execution_id,
                        attempt_number=attempt_num,
                        timestamp=attempt_start_ts,
                    )
                )

                try:
                    resolved_params = self._resolver.resolve(
                        step_def.parameters,
                        exec_context,
                        step_id=step_def.step_id,
                    )
                    resolved_step = step_def.model_copy(
                        update={"parameters": resolved_params},
                        deep=True,
                    )
                    executor = self._executor_registry.resolve(resolved_step.module_target)
                    outcome, elapsed_mono = execute_with_timeout(
                        executor,
                        resolved_step,
                        exec_context,
                        self._dependencies,
                        monotonic_clock=self._monotonic_clock,
                        step_execution_id=current_record.step_execution_id,
                    )

                    if outcome.status == ExecutionStatus.FAILED:
                        raise StepExecutionError(
                            outcome.error_message or f"Step '{step_id}' returned FAILED status.",
                            error_code=outcome.error_code,
                            workflow_id=workflow.workflow_id,
                            workflow_version=workflow.version,
                            execution_id=exec_id,
                            step_id=step_id,
                            step_execution_id=current_record.step_execution_id,
                        )

                    attempt_end_ts = max(
                        outcome.end_time or datetime.now(UTC),
                        attempt_start_ts,
                    )
                    attempt_duration = max(
                        elapsed_mono,
                        (attempt_end_ts - attempt_start_ts).total_seconds(),
                    )
                    succeeded_attempt = StepAttempt(
                        attempt_number=attempt_num,
                        status=ExecutionStatus.SUCCEEDED,
                        start_time=attempt_start_ts,
                        end_time=attempt_end_ts,
                        duration_seconds=attempt_duration,
                        timed_out=False,
                    )
                    step_with_attempt = self._tracker.record_step_attempt(
                        current_record.step_execution_id,
                        succeeded_attempt,
                    )

                    outputs = copy.deepcopy(outcome.output_references)
                    # Commit outputs to ExecutionContext only after step attempt succeeds
                    exec_context.set_step_output(step_id, outputs)

                    step_end_ts = max(attempt_end_ts, step_first_start_ts)
                    total_step_duration = max(
                        self._monotonic_clock() - step_first_start_mono,
                        (step_end_ts - step_first_start_ts).total_seconds(),
                    )
                    succeeded_record = step_with_attempt.model_copy(
                        update={
                            "status": ExecutionStatus.SUCCEEDED,
                            "attempt_number": attempt_num,
                            "start_time": step_first_start_ts,
                            "end_time": step_end_ts,
                            "duration_seconds": total_step_duration,
                            "module_execution_ref": outcome.module_execution_ref,
                            "error_code": None,
                            "error_message": None,
                            "output_references": outputs,
                        },
                        deep=True,
                    )
                    step_records[step_id] = self._tracker.update_step_execution(succeeded_record)

                    self._event_publisher.emit(
                        StepSucceeded(
                            execution_id=exec_id,
                            workflow_id=workflow.workflow_id,
                            workflow_version=workflow.version,
                            step_id=step_id,
                            step_execution_id=current_record.step_execution_id,
                            attempt_number=attempt_num,
                            duration_seconds=attempt_duration,
                            timestamp=step_end_ts,
                        )
                    )
                    break

                except Exception as exc:
                    attempt_end_ts = max(datetime.now(UTC), attempt_start_ts)
                    timeout_exc = exc if isinstance(exc, StepTimeoutError) else None
                    is_timeout = timeout_exc is not None
                    error_code = str(getattr(exc, "error_code", None) or type(exc).__name__)
                    error_msg = str(exc) or type(exc).__name__
                    attempt_duration = max(
                        0.0,
                        (attempt_end_ts - attempt_start_ts).total_seconds(),
                    )
                    if timeout_exc is not None and timeout_exc.timeout_seconds is not None:
                        attempt_duration = max(attempt_duration, float(timeout_exc.timeout_seconds))

                    failed_attempt = StepAttempt(
                        attempt_number=attempt_num,
                        status=ExecutionStatus.FAILED,
                        start_time=attempt_start_ts,
                        end_time=attempt_end_ts,
                        duration_seconds=attempt_duration,
                        error_code=error_code,
                        error_message=error_msg,
                        timed_out=is_timeout,
                    )
                    step_with_failed_attempt = self._tracker.record_step_attempt(
                        current_record.step_execution_id,
                        failed_attempt,
                    )
                    step_records[step_id] = step_with_failed_attempt

                    if timeout_exc is not None:
                        self._event_publisher.emit(
                            StepTimedOut(
                                execution_id=exec_id,
                                workflow_id=workflow.workflow_id,
                                workflow_version=workflow.version,
                                step_id=step_id,
                                step_execution_id=current_record.step_execution_id,
                                attempt_number=attempt_num,
                                timeout_seconds=(
                                    timeout_exc.timeout_seconds or step_def.timeout_seconds
                                ),
                                duration_seconds=attempt_duration,
                                error_code=error_code,
                                error_message=error_msg,
                                timestamp=attempt_end_ts,
                            )
                        )

                    can_retry = attempt_num < max_attempts and is_retryable_exception(exc)
                    if can_retry:
                        delay = retry_policy.calculate_delay(attempt_num - 1)
                        self._event_publisher.emit(
                            StepRetryScheduled(
                                execution_id=exec_id,
                                workflow_id=workflow.workflow_id,
                                workflow_version=workflow.version,
                                step_id=step_id,
                                step_execution_id=current_record.step_execution_id,
                                attempt_number=attempt_num,
                                delay_seconds=delay,
                                next_attempt_number=attempt_num + 1,
                                max_attempts=max_attempts,
                                error_code=error_code,
                                error_message=error_msg,
                                timestamp=attempt_end_ts,
                            )
                        )
                        if delay > 0.0:
                            self._sleep_fn(delay)
                        continue

                    # Terminal failure for this step
                    fail_end_ts = max(attempt_end_ts, step_first_start_ts)
                    total_step_duration = max(
                        self._monotonic_clock() - step_first_start_mono,
                        attempt_duration,
                        (fail_end_ts - step_first_start_ts).total_seconds(),
                    )
                    failed_record = step_with_failed_attempt.model_copy(
                        update={
                            "status": ExecutionStatus.FAILED,
                            "attempt_number": attempt_num,
                            "start_time": step_first_start_ts,
                            "end_time": fail_end_ts,
                            "duration_seconds": total_step_duration,
                            "error_code": error_code,
                            "error_message": error_msg,
                        },
                        deep=True,
                    )
                    step_records[step_id] = self._tracker.update_step_execution(failed_record)

                    self._event_publisher.emit(
                        StepFailed(
                            execution_id=exec_id,
                            workflow_id=workflow.workflow_id,
                            workflow_version=workflow.version,
                            step_id=step_id,
                            step_execution_id=current_record.step_execution_id,
                            attempt_number=attempt_num,
                            duration_seconds=total_step_duration,
                            error_code=error_code,
                            error_message=error_msg,
                            timestamp=fail_end_ts,
                        )
                    )

                    # Cancel all subsequent unexecuted steps
                    cancel_ts = datetime.now(UTC)
                    for remaining_id in topological_order[idx + 1 :]:
                        rem_record = step_records[remaining_id]
                        cancelled_record = rem_record.model_copy(
                            update={
                                "status": ExecutionStatus.CANCELLED,
                                "end_time": cancel_ts,
                            },
                            deep=True,
                        )
                        step_records[remaining_id] = self._tracker.update_step_execution(
                            cancelled_record
                        )
                        self._event_publisher.emit(
                            StepCancelled(
                                execution_id=exec_id,
                                workflow_id=workflow.workflow_id,
                                workflow_version=workflow.version,
                                step_id=remaining_id,
                                step_execution_id=rem_record.step_execution_id,
                                timestamp=cancel_ts,
                            )
                        )

                    self._tracker.fail(exec_id, error_msg)
                    failed_wf = self._tracker.get_workflow_execution(exec_id)
                    wf_end_time = failed_wf.end_time or datetime.now(UTC)
                    duration = max(
                        self._monotonic_clock() - wf_start_mono,
                        (wf_end_time - wf_start_time).total_seconds(),
                    )

                    self._event_publisher.emit(
                        WorkflowFailed(
                            execution_id=exec_id,
                            workflow_id=workflow.workflow_id,
                            workflow_version=workflow.version,
                            step_id=step_id,
                            step_execution_id=current_record.step_execution_id,
                            attempt_number=attempt_num,
                            duration_seconds=duration,
                            error_code=error_code,
                            error_message=error_msg,
                            timestamp=wf_end_time,
                        )
                    )

                    if raise_on_error:
                        raise

                    tracked_steps = self._tracker.list_step_executions(exec_id)
                    return WorkflowResult(
                        execution_id=exec_id,
                        workflow_id=workflow.workflow_id,
                        status=ExecutionStatus.FAILED,
                        step_results=tracked_steps,
                        duration=duration,
                        message=error_msg,
                    )

        # 4. All steps succeeded -> complete WorkflowExecution
        self._tracker.complete(exec_id)
        completed_wf = self._tracker.get_workflow_execution(exec_id)
        wf_end_time = completed_wf.end_time or datetime.now(UTC)
        duration = max(
            self._monotonic_clock() - wf_start_mono,
            (wf_end_time - wf_start_time).total_seconds(),
        )
        tracked_steps = self._tracker.list_step_executions(exec_id)

        self._event_publisher.emit(
            WorkflowSucceeded(
                execution_id=exec_id,
                workflow_id=workflow.workflow_id,
                workflow_version=workflow.version,
                duration_seconds=duration,
                timestamp=wf_end_time,
            )
        )

        return WorkflowResult(
            execution_id=exec_id,
            workflow_id=workflow.workflow_id,
            status=ExecutionStatus.SUCCEEDED,
            step_results=tracked_steps,
            duration=duration,
            message="Workflow execution succeeded.",
        )


__all__ = ["WorkflowEngine"]
