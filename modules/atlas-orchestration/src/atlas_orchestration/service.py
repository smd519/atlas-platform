"""Public orchestration service entry point for atlas-orchestration Phase 1."""

from __future__ import annotations

from atlas_core import ExecutionStatus

from atlas_orchestration.dependencies import OrchestrationDependencies
from atlas_orchestration.models.step_execution import StepExecution
from atlas_orchestration.models.workflow import WorkflowDefinition
from atlas_orchestration.models.workflow_execution import WorkflowExecution
from atlas_orchestration.registry import WorkflowRegistry
from atlas_orchestration.tracking.execution_tracker import ExecutionTracker
from atlas_orchestration.tracking.memory import InMemoryExecutionTracker
from atlas_orchestration.validation.workflow_validator import (
    WorkflowValidationResult,
    WorkflowValidator,
)


class OrchestrationService:
    """Primary public service interface for workflow registration, validation, and tracking.

    Phase 1 provides the domain foundation: workflow validation, versioned registration,
    dependency injection wiring, and execution tracking inspection. Actual workflow
    execution is deferred to Phase 2.
    """

    def __init__(
        self,
        *,
        registry: WorkflowRegistry | None = None,
        validator: WorkflowValidator | None = None,
        tracker: ExecutionTracker | None = None,
        dependencies: OrchestrationDependencies | None = None,
    ) -> None:
        self._validator = validator or (
            registry.validator if registry is not None else WorkflowValidator()
        )
        self._registry = registry or WorkflowRegistry(validator=self._validator)
        self._tracker: ExecutionTracker = tracker or InMemoryExecutionTracker()
        self._dependencies = dependencies or OrchestrationDependencies()

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

