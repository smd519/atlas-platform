"""Sequential workflow execution engine for atlas-orchestration."""

from __future__ import annotations

import copy
from datetime import UTC, datetime
from typing import Any

from atlas_core import ExecutionStatus

from atlas_orchestration.dependencies import OrchestrationDependencies
from atlas_orchestration.exceptions import StepExecutionError
from atlas_orchestration.execution.context import ExecutionContext
from atlas_orchestration.execution.executor import invoke_step_executor
from atlas_orchestration.execution.registry import StepExecutorRegistry
from atlas_orchestration.execution.resolver import ContextResolver
from atlas_orchestration.models.step_execution import StepExecution
from atlas_orchestration.models.workflow import WorkflowDefinition
from atlas_orchestration.models.workflow_execution import WorkflowExecution
from atlas_orchestration.models.workflow_result import WorkflowResult
from atlas_orchestration.tracking.execution_tracker import ExecutionTracker
from atlas_orchestration.tracking.memory import InMemoryExecutionTracker
from atlas_orchestration.validation.workflow_validator import WorkflowValidator


class WorkflowEngine:
    """Sequential workflow execution engine.

    Coordinates workflow validation, topological ordering, execution context
    initialization, dynamic parameter resolution, step execution, fail-fast
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
    ) -> None:
        self._executor_registry = executor_registry
        self._validator = validator or WorkflowValidator()
        self._resolver = resolver or ContextResolver()
        self._tracker: ExecutionTracker = tracker or InMemoryExecutionTracker()
        self._dependencies = dependencies or OrchestrationDependencies()

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
            step_start_ts = datetime.now(UTC)

            running_record = current_record.model_copy(
                update={
                    "status": ExecutionStatus.RUNNING,
                    "start_time": step_start_ts,
                },
                deep=True,
            )
            step_records[step_id] = self._tracker.update_step_execution(running_record)

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
                outcome = invoke_step_executor(
                    executor,
                    resolved_step,
                    exec_context,
                    self._dependencies,
                )

                if outcome.status == ExecutionStatus.FAILED:
                    raise StepExecutionError(
                        outcome.error_message or f"Step '{step_id}' returned FAILED status.",
                        workflow_id=workflow.workflow_id,
                        workflow_version=workflow.version,
                        execution_id=exec_id,
                        step_id=step_id,
                        step_execution_id=current_record.step_execution_id,
                    )

                outputs = copy.deepcopy(outcome.output_references)
                exec_context.set_step_output(step_id, outputs)

                step_end_ts = max(
                    outcome.end_time or datetime.now(UTC),
                    step_start_ts,
                )
                succeeded_record = step_records[step_id].model_copy(
                    update={
                        "status": ExecutionStatus.SUCCEEDED,
                        "start_time": step_start_ts,
                        "end_time": step_end_ts,
                        "module_execution_ref": outcome.module_execution_ref,
                        "output_references": outputs,
                    },
                    deep=True,
                )
                step_records[step_id] = self._tracker.update_step_execution(succeeded_record)

            except Exception as exc:
                fail_end_ts = max(datetime.now(UTC), step_start_ts)
                error_code = getattr(exc, "error_code", None) or type(exc).__name__
                error_msg = str(exc) or type(exc).__name__

                failed_record = step_records[step_id].model_copy(
                    update={
                        "status": ExecutionStatus.FAILED,
                        "start_time": step_start_ts,
                        "end_time": fail_end_ts,
                        "error_code": str(error_code),
                        "error_message": error_msg,
                    },
                    deep=True,
                )
                step_records[step_id] = self._tracker.update_step_execution(failed_record)

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

                self._tracker.fail(exec_id, error_msg)
                if raise_on_error:
                    raise

                failed_wf = self._tracker.get_workflow_execution(exec_id)
                wf_end_time = failed_wf.end_time or datetime.now(UTC)
                duration = max(0.0, (wf_end_time - wf_start_time).total_seconds())
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
        duration = max(0.0, (wf_end_time - wf_start_time).total_seconds())
        tracked_steps = self._tracker.list_step_executions(exec_id)

        return WorkflowResult(
            execution_id=exec_id,
            workflow_id=workflow.workflow_id,
            status=ExecutionStatus.SUCCEEDED,
            step_results=tracked_steps,
            duration=duration,
            message="Workflow execution succeeded.",
        )


__all__ = ["WorkflowEngine"]
