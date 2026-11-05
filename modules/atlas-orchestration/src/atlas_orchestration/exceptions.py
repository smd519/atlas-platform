"""Domain exception hierarchy for atlas-orchestration."""

from __future__ import annotations

from atlas_core import OrchestrationError as CoreOrchestrationError


class OrchestrationError(CoreOrchestrationError):
    """Base exception for all atlas-orchestration errors."""

    def __init__(
        self,
        message: str,
        *,
        workflow_id: str | None = None,
        workflow_version: int | None = None,
        execution_id: str | None = None,
        step_id: str | None = None,
        step_execution_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.workflow_id = workflow_id
        self.workflow_version = workflow_version
        self.execution_id = execution_id
        self.step_id = step_id
        self.step_execution_id = step_execution_id


class WorkflowNotFoundError(OrchestrationError):
    """Raised when a requested workflow definition or version is not found in the registry."""


class DuplicateWorkflowError(OrchestrationError):
    """Raised when attempting to register a workflow version that already exists."""


class WorkflowVersionConflictError(DuplicateWorkflowError):
    """Raised when a (workflow_id, version) registration conflicts with an existing version."""


class InvalidWorkflowDefinitionError(OrchestrationError):
    """Raised when a workflow definition violates structural or metadata constraints."""


class InvalidStepDefinitionError(InvalidWorkflowDefinitionError):
    """Raised when a workflow step definition is invalid."""


class InvalidWorkflowDependencyError(InvalidWorkflowDefinitionError):
    """Raised when a step dependency reference is missing, duplicated, or self-referential."""


class CyclicDependencyError(InvalidWorkflowDependencyError):
    """Raised when a cycle is detected in the workflow step dependency graph."""

    def __init__(
        self,
        message: str,
        *,
        cycle: tuple[str, ...] = (),
        workflow_id: str | None = None,
        workflow_version: int | None = None,
    ) -> None:
        super().__init__(
            message,
            workflow_id=workflow_id,
            workflow_version=workflow_version,
        )
        self.cycle = cycle


class StepExecutionError(OrchestrationError):
    """Raised when a workflow step execution or step state transition fails."""


class InvalidWorkflowStateError(OrchestrationError):
    """Raised when a workflow or step execution record violates state or invariant constraints."""


class WorkflowExecutionNotFoundError(OrchestrationError):
    """Raised when a requested workflow execution record cannot be found."""


class StepExecutionNotFoundError(OrchestrationError):
    """Raised when a requested step execution record cannot be found."""


class ExecutionContextError(OrchestrationError):
    """Raised when an execution context lookup or value validation fails."""


class UnresolvedContextReferenceError(ExecutionContextError):
    """Raised when a dynamic context reference (${...}) cannot be parsed or resolved."""

    def __init__(
        self,
        message: str,
        *,
        reference: str | None = None,
        workflow_id: str | None = None,
        workflow_version: int | None = None,
        execution_id: str | None = None,
        step_id: str | None = None,
    ) -> None:
        super().__init__(
            message,
            workflow_id=workflow_id,
            workflow_version=workflow_version,
            execution_id=execution_id,
            step_id=step_id,
        )
        self.reference = reference


class UnknownModuleTargetError(OrchestrationError):
    """Raised when no step executor is registered for a step's module_target."""

    def __init__(
        self,
        message: str,
        *,
        module_target: str | None = None,
        workflow_id: str | None = None,
        workflow_version: int | None = None,
        execution_id: str | None = None,
        step_id: str | None = None,
    ) -> None:
        super().__init__(
            message,
            workflow_id=workflow_id,
            workflow_version=workflow_version,
            execution_id=execution_id,
            step_id=step_id,
        )
        self.module_target = module_target


class DuplicateExecutorRegistrationError(OrchestrationError):
    """Raised when attempting to register an executor for an already-registered module_target."""

    def __init__(
        self,
        message: str,
        *,
        module_target: str | None = None,
    ) -> None:
        super().__init__(message)
        self.module_target = module_target


__all__ = [
    "CyclicDependencyError",
    "DuplicateExecutorRegistrationError",
    "DuplicateWorkflowError",
    "ExecutionContextError",
    "InvalidStepDefinitionError",
    "InvalidWorkflowDefinitionError",
    "InvalidWorkflowDependencyError",
    "InvalidWorkflowStateError",
    "OrchestrationError",
    "StepExecutionError",
    "StepExecutionNotFoundError",
    "UnknownModuleTargetError",
    "UnresolvedContextReferenceError",
    "WorkflowExecutionNotFoundError",
    "WorkflowNotFoundError",
    "WorkflowVersionConflictError",
]
