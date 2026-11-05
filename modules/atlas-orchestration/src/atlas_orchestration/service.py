"""Public orchestration service entry point for atlas-orchestration."""

from __future__ import annotations

from typing import Any

from atlas_core import ExecutionStatus

from atlas_orchestration.dependencies import OrchestrationDependencies
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
from atlas_orchestration.models.step_execution import StepExecution
from atlas_orchestration.models.workflow import WorkflowDefinition
from atlas_orchestration.models.workflow_execution import WorkflowExecution
from atlas_orchestration.models.workflow_result import WorkflowResult
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


__all__ = ["OrchestrationService"]
