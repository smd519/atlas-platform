"""Core execution runner coordinating YAML loading and workflow orchestration."""

from __future__ import annotations

from pathlib import Path

from atlas_core import ExecutionStatus
from atlas_orchestration import (
    ExecutionContext,
    OrchestrationDependencies,
    OrchestrationService,
    WorkflowResult,
)

from atlas_runner.config import load_job_definition, load_workflow_definition
from atlas_runner.dependencies import (
    build_default_dependencies,
    create_orchestration_service,
)
from atlas_runner.exceptions import RunnerExecutionError


class AtlasRunner:
    """Loads customer YAML configs and executes workflows via :class:`OrchestrationService`."""

    def __init__(
        self,
        *,
        orchestration_service: OrchestrationService | None = None,
        dependencies: OrchestrationDependencies | None = None,
    ) -> None:
        if orchestration_service is not None:
            self._orchestrator = orchestration_service
        else:
            resolved_deps = dependencies or build_default_dependencies()
            self._orchestrator = create_orchestration_service(dependencies=resolved_deps)

    @property
    def orchestrator(self) -> OrchestrationService:
        """Return the underlying :class:`OrchestrationService`."""
        return self._orchestrator

    def run(
        self,
        workspace: str | Path,
        job_path: str | Path,
        *,
        raise_on_error: bool = False,
    ) -> WorkflowResult:
        """Load a job and its referenced workflow from ``workspace`` and execute it."""
        job_def, _ = load_job_definition(workspace, job_path)
        workflow_def, merged_params, _ = load_workflow_definition(
            workspace,
            job_def.workflow_ref,
            job_parameters=job_def.parameters,
        )

        self._orchestrator.register_workflow(workflow_def)

        context = ExecutionContext(
            workflow_id=workflow_def.workflow_id,
            workflow_version=workflow_def.version,
            execution_metadata={"job_id": job_def.job_id},
        )

        result = self._orchestrator.execute_workflow(
            workflow_def.workflow_id,
            version=workflow_def.version,
            parameters=merged_params,
            context=context,
        )

        if raise_on_error and result.status != ExecutionStatus.SUCCEEDED:
            raise RunnerExecutionError(
                result.message
                or f"Job '{job_def.job_id}' (workflow '{workflow_def.workflow_id}') failed."
            )

        return result


__all__ = ["AtlasRunner"]
