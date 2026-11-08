"""Stateless runner coordinating validation, workflow assembly, and orchestration handoff."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from atlas_core import ExecutionStatus
from atlas_orchestration import (
    JobResult,
    OrchestrationDependencies,
    OrchestrationService,
    WorkflowDefinition,
    WorkflowValidator,
)

from atlas_runner.config import (
    load_job_definition,
    load_workflow_definition,
    resolve_workspace,
    validate_workflow_dag,
)
from atlas_runner.dependencies import (
    build_default_dependencies,
    create_orchestration_service,
)
from atlas_runner.exceptions import RunnerExecutionError
from atlas_runner.models import ValidationReport


class AtlasRunner:
    """Stateless YAML job runner that assembles workflows and hands off to orchestration."""

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
        """Validate Job YAML, Workflow YAMLs, parameter merge, path isolation, and DAG structure.

        Does NOT instantiate storage, connectors, ingestion, processing, quality, or
        orchestration execution services.
        """
        workspace_context = resolve_workspace(workspace, field="workspace")
        workspace_root = workspace_context.root_path
        job_def, resolved_job_path = load_job_definition(workspace_root, job_path)
        self._last_job_id = job_def.job_id

        workflows: list[WorkflowDefinition] = []
        workflow_files: list[Path] = []
        topological_orders: dict[str, tuple[str, ...]] = {}
        all_valid = True

        for wf_ref in job_def.workflows:
            workflow_def, _, resolved_wf_path = load_workflow_definition(
                workspace_root,
                wf_ref,
                job_parameters=job_def.parameters,
                job_file=resolved_job_path,
                field="workflow_ref",
            )
            dag_result = validate_workflow_dag(
                workflow_def,
                config_file=resolved_wf_path,
                validator=validator,
            )
            all_valid = all_valid and dag_result.is_valid
            workflows.append(workflow_def)
            workflow_files.append(resolved_wf_path)
            topological_orders[workflow_def.workflow_id] = dag_result.topological_order

        first_wf = workflows[0]
        first_file = workflow_files[0]
        first_order = topological_orders[first_wf.workflow_id]

        return ValidationReport(
            is_valid=all_valid,
            job_id=job_def.job_id,
            job_file=resolved_job_path,
            workflow_id=first_wf.workflow_id,
            workflow_version=first_wf.version,
            workflow_file=first_file,
            topological_order=first_order,
            workflow=first_wf,
            workflows=workflows,
            workflow_files=workflow_files,
            topological_orders=topological_orders,
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
    ) -> JobResult:
        """Assemble Job workflows, build a fresh dependency graph, and hand off to orchestration."""
        workspace_context = resolve_workspace(workspace, field="workspace")

        job_def, resolved_job_path = load_job_definition(workspace_context.root_path, job_path)
        self._last_job_id = job_def.job_id

        workflows: list[WorkflowDefinition] = []
        for wf_ref in job_def.workflows:
            workflow_def, _, resolved_wf_path = load_workflow_definition(
                workspace_context.root_path,
                wf_ref,
                job_parameters=job_def.parameters,
                job_file=resolved_job_path,
                field="workflow_ref",
            )
            validate_workflow_dag(workflow_def, config_file=resolved_wf_path)
            workflows.append(workflow_def)

        deps, orchestrator = self._build_execution_runtime()
        self._last_dependencies = deps
        self._last_orchestrator = orchestrator

        result = orchestrator.execute_job(
            job_id=job_def.job_id,
            workflows=workflows,
            workspace=workspace_context,
        )

        if raise_on_error and result.status != ExecutionStatus.SUCCEEDED:
            raise RunnerExecutionError(
                result.message or f"Job '{job_def.job_id}' execution failed."
            )

        return result


__all__ = ["AtlasRunner"]
