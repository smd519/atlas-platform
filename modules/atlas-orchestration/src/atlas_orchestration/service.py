"""Public orchestration service entry point for atlas-orchestration."""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from atlas_core import ExecutionStatus, generate_execution_id, require_not_empty

from atlas_orchestration.dependencies import OrchestrationDependencies
from atlas_orchestration.exceptions import (
    InvalidWorkflowDefinitionError,
    InvalidWorkflowStateError,
)
from atlas_orchestration.execution.context import ExecutionContext
from atlas_orchestration.execution.engine import WorkflowEngine
from atlas_orchestration.execution.executor import BaseStepExecutor
from atlas_orchestration.execution.executors import (
    IngestionExecutor,
    ProcessingExecutor,
    QualityExecutor,
)
from atlas_orchestration.execution.registry import StepExecutorRegistry
from atlas_orchestration.execution.resolver import ContextResolver
from atlas_orchestration.models.events import EventSubscriber, ExecutionEventPublisher
from atlas_orchestration.models.job_result import JobResult
from atlas_orchestration.models.step_execution import StepAttempt, StepExecution
from atlas_orchestration.models.workflow import WorkflowDefinition
from atlas_orchestration.models.workflow_execution import WorkflowExecution
from atlas_orchestration.models.workflow_result import WorkflowResult
from atlas_orchestration.models.workspace import WorkspaceContext
from atlas_orchestration.registry import WorkflowRegistry
from atlas_orchestration.tracking.execution_tracker import ExecutionTracker
from atlas_orchestration.tracking.memory import InMemoryExecutionTracker
from atlas_orchestration.validation.workflow_validator import (
    WorkflowValidationResult,
    WorkflowValidator,
)


class OrchestrationService:
    """Primary public service interface for workflow registration, validation, and execution."""

    def __init__(
        self,
        *,
        registry: WorkflowRegistry | None = None,
        validator: WorkflowValidator | None = None,
        tracker: ExecutionTracker | None = None,
        dependencies: OrchestrationDependencies | None = None,
        executor_registry: StepExecutorRegistry | None = None,
        resolver: ContextResolver | None = None,
        event_publisher: ExecutionEventPublisher | None = None,
        sleep_fn: Callable[[float], None] = time.sleep,
        monotonic_clock: Callable[[], float] = time.monotonic,
        engine: WorkflowEngine | None = None,
        register_default_executors: bool = True,
    ) -> None:
        self._validator = validator or (
            registry.validator if registry is not None else WorkflowValidator()
        )
        self._registry = registry or WorkflowRegistry(validator=self._validator)
        self._tracker: ExecutionTracker = tracker or InMemoryExecutionTracker()
        self._dependencies = dependencies or OrchestrationDependencies()
        self._resolver = resolver or ContextResolver()
        self._event_publisher = event_publisher or ExecutionEventPublisher()
        self._monotonic_clock = monotonic_clock

        if executor_registry is not None:
            self._executor_registry = executor_registry
        else:
            self._executor_registry = StepExecutorRegistry()
            if register_default_executors:
                self._register_default_executors(self._executor_registry)

        self._engine = engine or WorkflowEngine(
            executor_registry=self._executor_registry,
            validator=self._validator,
            resolver=self._resolver,
            tracker=self._tracker,
            dependencies=self._dependencies,
            event_publisher=self._event_publisher,
            sleep_fn=sleep_fn,
            monotonic_clock=monotonic_clock,
        )

    @staticmethod
    def _register_default_executors(registry: StepExecutorRegistry) -> None:
        registry.register("atlas-ingestion", IngestionExecutor(), overwrite=True)
        registry.register("atlas-processing", ProcessingExecutor(), overwrite=True)
        registry.register("atlas-quality", QualityExecutor(), overwrite=True)

    @property
    def registry(self) -> WorkflowRegistry:
        """Return the underlying :class:`WorkflowRegistry`."""
        return self._registry

    @property
    def validator(self) -> WorkflowValidator:
        """Return the underlying :class:`WorkflowValidator`."""
        return self._validator

    @property
    def tracker(self) -> ExecutionTracker:
        """Return the underlying :class:`ExecutionTracker`."""
        return self._tracker

    @property
    def dependencies(self) -> OrchestrationDependencies:
        """Return the injected :class:`OrchestrationDependencies` container."""
        return self._dependencies

    @property
    def executor_registry(self) -> StepExecutorRegistry:
        """Return the underlying :class:`StepExecutorRegistry`."""
        return self._executor_registry

    @property
    def resolver(self) -> ContextResolver:
        """Return the underlying :class:`ContextResolver`."""
        return self._resolver

    @property
    def engine(self) -> WorkflowEngine:
        """Return the underlying :class:`WorkflowEngine`."""
        return self._engine

    @property
    def event_publisher(self) -> ExecutionEventPublisher:
        """Return the :class:`ExecutionEventPublisher` used by this service."""
        return self._engine.event_publisher

    def register_event_listener(self, listener: EventSubscriber) -> None:
        """Register a listener to receive structured execution events."""
        self._engine.register_event_listener(listener)

    def unregister_event_listener(self, listener: EventSubscriber) -> None:
        """Remove a previously registered execution event listener."""
        self._engine.unregister_event_listener(listener)

    # ------------------------------------------------------------------
    # Workflow Validation & Registration
    # ------------------------------------------------------------------

    def validate_workflow(self, workflow: WorkflowDefinition) -> WorkflowValidationResult:
        """Validate ``workflow`` structure, dependencies, and DAG acyclicity."""
        return self._validator.validate(workflow)

    def register_workflow(self, workflow: WorkflowDefinition) -> WorkflowDefinition:
        """Validate and register a versioned :class:`WorkflowDefinition`."""
        return self._registry.register(workflow)

    def get_workflow(
        self,
        workflow_id: str,
        version: int | None = None,
    ) -> WorkflowDefinition:
        """Retrieve a workflow definition by ``workflow_id`` and optional ``version``."""
        return self._registry.get(workflow_id, version=version)

    def get_latest_workflow(self, workflow_id: str) -> WorkflowDefinition:
        """Retrieve the highest registered version of ``workflow_id``."""
        return self._registry.get_latest(workflow_id)

    def list_workflows(self, workflow_id: str | None = None) -> list[WorkflowDefinition]:
        """List registered workflows ordered by ``(workflow_id, version)``."""
        return self._registry.list_workflows(workflow_id=workflow_id)

    def unregister_workflow(
        self,
        workflow_id: str,
        version: int | None = None,
    ) -> None:
        """Remove a specific workflow version or all versions of ``workflow_id``."""
        self._registry.unregister(workflow_id, version=version)

    # ------------------------------------------------------------------
    # Step Executor Registration & Sequential Workflow Execution
    # ------------------------------------------------------------------

    def register_executor(
        self,
        module_target: str,
        executor: BaseStepExecutor,
        *,
        overwrite: bool = False,
    ) -> None:
        """Register a :class:`BaseStepExecutor` for ``module_target``."""
        self._executor_registry.register(module_target, executor, overwrite=overwrite)

    def execute_workflow(
        self,
        workflow: WorkflowDefinition | str,
        *,
        version: int | None = None,
        parameters: dict[str, Any] | None = None,
        execution_id: str | None = None,
        correlation_id: str | None = None,
        context: ExecutionContext | None = None,
        raise_on_error: bool = False,
    ) -> WorkflowResult:
        """Execute a :class:`WorkflowDefinition` (or registered ``workflow_id``) sequentially."""
        resolved_wf = (
            self._registry.get(workflow, version=version) if isinstance(workflow, str) else workflow
        )
        return self._engine.execute(
            resolved_wf,
            parameters=parameters,
            execution_id=execution_id,
            correlation_id=correlation_id,
            context=context,
            raise_on_error=raise_on_error,
        )

    def execute_job(
        self,
        job_id: str,
        workflows: list[WorkflowDefinition],
        workspace: WorkspaceContext,
    ) -> JobResult:
        """Execute a batch of :class:`WorkflowDefinition` objects sequentially for ``job_id``.

        Failure Semantics (Phase 1 — Fail-Fast Sequential Batch Execution)
        ------------------------------------------------------------------
        - Workflows execute sequentially in the exact order provided in ``workflows``.
        - Each workflow is registered via :meth:`register_workflow` and executed via
          :meth:`execute_workflow` using the existing internal DAG engine.
        - If a workflow fails (status != ``SUCCEEDED``), its :class:`WorkflowResult` is
          recorded in ``JobResult.workflow_results``, execution stops immediately
          (subsequent workflows are not executed), and the parent Job execution is
          finalized with ``ExecutionStatus.FAILED``.
        - If all workflows succeed, the parent Job execution is finalized with
          ``ExecutionStatus.SUCCEEDED``.
        """
        clean_job_id = job_id.strip() if isinstance(job_id, str) else ""
        try:
            require_not_empty(clean_job_id, "job_id")
        except ValueError as exc:
            raise InvalidWorkflowDefinitionError(str(exc)) from exc

        if not isinstance(workspace, WorkspaceContext):
            raise InvalidWorkflowStateError(
                f"workspace must be a WorkspaceContext instance, got {type(workspace).__name__}."
            )

        if not isinstance(workflows, list) or len(workflows) == 0:
            raise InvalidWorkflowDefinitionError(
                f"Job '{clean_job_id}' must contain at least one WorkflowDefinition."
            )

        for idx, wf in enumerate(workflows):
            if not isinstance(wf, WorkflowDefinition):
                raise InvalidWorkflowDefinitionError(
                    f"Workflow at index {idx} in job '{clean_job_id}' must be a "
                    f"WorkflowDefinition, got {type(wf).__name__}."
                )

        job_execution_id = generate_execution_id()
        parent_execution = WorkflowExecution(
            execution_id=job_execution_id,
            workflow_id=clean_job_id,
            workflow_version=1,
            status=ExecutionStatus.PENDING,
            context_overrides={
                "execution_type": "job",
                "job_id": clean_job_id,
                "workspace": str(workspace.root_path),
                "workflow_ids": [wf.workflow_id for wf in workflows],
            },
        )
        self._tracker.create_workflow_execution(parent_execution)
        self._tracker.start(job_execution_id)
        running_parent = self._tracker.get_workflow_execution(job_execution_id)
        job_start_time = running_parent.start_time or datetime.now(UTC)
        job_start_mono = self._monotonic_clock()

        workflow_results: list[WorkflowResult] = []
        failed_reason: str | None = None

        try:
            for workflow in workflows:
                self.register_workflow(workflow)
                wf_context = ExecutionContext(
                    workflow_id=workflow.workflow_id,
                    workflow_version=workflow.version,
                    correlation_id=job_execution_id,
                    execution_metadata={
                        "job_id": clean_job_id,
                        "job_execution_id": job_execution_id,
                        "workspace": str(workspace.root_path),
                    },
                )
                wf_result = self.execute_workflow(
                    workflow.workflow_id,
                    version=workflow.version,
                    context=wf_context,
                    raise_on_error=False,
                )
                workflow_results.append(wf_result)

                if wf_result.status != ExecutionStatus.SUCCEEDED:
                    failed_reason = (
                        wf_result.message
                        or f"Workflow '{workflow.workflow_id}' failed in job '{clean_job_id}'."
                    )
                    break
        except Exception as exc:
            err_msg = str(exc) or type(exc).__name__
            self._tracker.fail(job_execution_id, err_msg)
            raise

        if failed_reason is not None:
            self._tracker.fail(job_execution_id, failed_reason)
            failed_parent = self._tracker.get_workflow_execution(job_execution_id)
            job_end_time = failed_parent.end_time or datetime.now(UTC)
            duration = max(
                self._monotonic_clock() - job_start_mono,
                sum(r.duration for r in workflow_results),
                (job_end_time - job_start_time).total_seconds(),
            )
            return JobResult(
                job_id=clean_job_id,
                execution_id=job_execution_id,
                status=ExecutionStatus.FAILED,
                workflow_results=workflow_results,
                start_time=job_start_time,
                end_time=job_end_time,
                duration=duration,
                message=failed_reason,
            )

        self._tracker.complete(job_execution_id)
        completed_parent = self._tracker.get_workflow_execution(job_execution_id)
        job_end_time = completed_parent.end_time or datetime.now(UTC)
        duration = max(
            self._monotonic_clock() - job_start_mono,
            sum(r.duration for r in workflow_results),
            (job_end_time - job_start_time).total_seconds(),
        )
        return JobResult(
            job_id=clean_job_id,
            execution_id=job_execution_id,
            status=ExecutionStatus.SUCCEEDED,
            workflow_results=workflow_results,
            start_time=job_start_time,
            end_time=job_end_time,
            duration=duration,
            message=(
                f"Job '{clean_job_id}' execution succeeded "
                f"({len(workflow_results)} workflow(s) completed)."
            ),
        )

    # ------------------------------------------------------------------
    # Execution Tracking Queries
    # ------------------------------------------------------------------

    def get_workflow_execution(self, execution_id: str) -> WorkflowExecution:
        """Retrieve a tracked :class:`WorkflowExecution` by ``execution_id``."""
        return self._tracker.get_workflow_execution(execution_id)

    def list_workflow_executions(
        self,
        workflow_id: str | None = None,
        status: ExecutionStatus | None = None,
    ) -> list[WorkflowExecution]:
        """List tracked workflow executions optionally filtered by workflow or status."""
        return self._tracker.list_workflow_executions(
            workflow_id=workflow_id,
            status=status,
        )

    def get_step_execution(self, step_execution_id: str) -> StepExecution:
        """Retrieve a tracked :class:`StepExecution` by ``step_execution_id``."""
        return self._tracker.get_step_execution(step_execution_id)

    def list_step_executions(self, execution_id: str) -> list[StepExecution]:
        """List all tracked :class:`StepExecution` records for ``execution_id``."""
        return self._tracker.list_step_executions(execution_id)

    def list_step_attempts(self, step_execution_id: str) -> list[StepAttempt]:
        """List all tracked :class:`StepAttempt` records for ``step_execution_id``."""
        return self._tracker.list_step_attempts(step_execution_id)


__all__ = ["OrchestrationService"]
