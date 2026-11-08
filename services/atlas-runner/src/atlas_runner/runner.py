"""Stateless runner coordinating validation, dependency construction, and orchestration."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from atlas_core import ExecutionStatus
from atlas_orchestration import (
    ExecutionContext,
    OrchestrationDependencies,
    OrchestrationService,
    WorkflowResult,
    WorkflowValidator,
)

from atlas_runner.config import (
    load_job_definition,
    load_workflow_definition,
    validate_workflow_dag,
)
from atlas_runner.dependencies import (
    build_default_dependencies,
    create_orchestration_service,
)
from atlas_runner.exceptions import RunnerExecutionError
from atlas_runner.models import ValidationReport


class AtlasRunner:
    """Stateless YAML workflow runner that constructs a fresh dependency graph per execution."""

    def __init__(
        self,
        *,
        orchestration_service: OrchestrationService | None = None,
        dependencies: OrchestrationDependencies | None = None,
        dependency_factory: Callable[[], OrchestrationDependencies] | None = None,
        orchestration_factory: (
            Callable[[OrchestrationDependencies], OrchestrationService] | None
        ) = None,
    ) -> None:
        self._injected_orchestrator = orchestration_service
        self._injected_dependencies = dependencies
        self._dependency_factory = dependency_factory or build_default_dependencies
        self._orchestration_factory = orchestration_factory or create_orchestration_service
        self._last_dependencies: OrchestrationDependencies | None = None
        self._last_orchestrator: OrchestrationService | None = None
        self._last_job_id: str | None = None

    @property
    def orchestrator(self) -> OrchestrationService | None:
        """Return the most recently used or injected :class:`OrchestrationService`."""
        return self._last_orchestrator or self._injected_orchestrator

    @property
    def last_dependencies(self) -> OrchestrationDependencies | None:
        """Return the :class:`OrchestrationDependencies` graph from the most recent ``run()``."""
        return self._last_dependencies

    @property
    def last_job_id(self) -> str | None:
        """Return the ``job_id`` from the most recent ``validate()`` or ``run()`` call."""
        return self._last_job_id

    def validate(
        self,
        workspace: str | Path,
        job_path: str | Path,
        *,
        validator: WorkflowValidator | None = None,
    ) -> ValidationReport:
        """Validate Job YAML, Workflow YAML, parameter merge, path isolation, and DAG structure.

        Does NOT instantiate storage, connectors, ingestion, processing, quality, or
        orchestration execution services.
        """
        job_def, resolved_job_path = load_job_definition(workspace, job_path)
        self._last_job_id = job_def.job_id

        workflow_def, _, resolved_wf_path = load_workflow_definition(
            workspace,
            job_def.workflow_ref,
            job_parameters=job_def.parameters,
            job_file=resolved_job_path,
        )

        dag_result = validate_workflow_dag(
            workflow_def,
            config_file=resolved_wf_path,
            validator=validator,
        )

        return ValidationReport(
            is_valid=dag_result.is_valid,
            job_id=job_def.job_id,
            job_file=resolved_job_path,
            workflow_id=workflow_def.workflow_id,
            workflow_version=workflow_def.version,
            workflow_file=resolved_wf_path,
            topological_order=dag_result.topological_order,
            workflow=workflow_def,
        )

    def _build_execution_runtime(
        self,
    ) -> tuple[OrchestrationDependencies, OrchestrationService]:
        """Instantiate a fresh dependency graph and orchestration service for a single run."""
        if self._injected_orchestrator is not None:
            deps = self._injected_dependencies or self._injected_orchestrator.dependencies
            return deps, self._injected_orchestrator

        deps = (
            self._injected_dependencies
            if self._injected_dependencies is not None
            else self._dependency_factory()
        )
        orchestrator = self._orchestration_factory(deps)
        return deps, orchestrator

    def run(
        self,
        workspace: str | Path,
        job_path: str | Path,
        *,
        raise_on_error: bool = False,
    ) -> WorkflowResult:
        """Validate configurations, construct a fresh dependency graph, and execute the workflow."""
        job_def, resolved_job_path = load_job_definition(workspace, job_path)
        self._last_job_id = job_def.job_id

        workflow_def, merged_params, resolved_wf_path = load_workflow_definition(
            workspace,
            job_def.workflow_ref,
            job_parameters=job_def.parameters,
            job_file=resolved_job_path,
        )
        validate_workflow_dag(workflow_def, config_file=resolved_wf_path)

        deps, orchestrator = self._build_execution_runtime()
        self._last_dependencies = deps
        self._last_orchestrator = orchestrator

        orchestrator.register_workflow(workflow_def)

        context = ExecutionContext(
            workflow_id=workflow_def.workflow_id,
            workflow_version=workflow_def.version,
            execution_metadata={"job_id": job_def.job_id},
        )

        result = orchestrator.execute_workflow(
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
