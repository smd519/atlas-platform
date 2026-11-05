"""Dedicated workflow structure, dependency, and DAG validator."""

from __future__ import annotations

from collections import deque

from pydantic import BaseModel, ConfigDict

from atlas_orchestration.exceptions import (
    CyclicDependencyError,
    InvalidStepDefinitionError,
    InvalidWorkflowDefinitionError,
    InvalidWorkflowDependencyError,
)
from atlas_orchestration.models.workflow import WorkflowDefinition
from atlas_orchestration.models.workflow_step import validate_json_compatible


class WorkflowValidationResult(BaseModel):
    """Immutable result produced by validating a :class:`WorkflowDefinition`."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    is_valid: bool = True
    workflow_id: str
    workflow_version: int
    topological_order: tuple[str, ...]


class WorkflowValidator:
    """Validates workflow structural constraints, dependency references, and DAG acyclicity."""

    def validate(self, workflow: WorkflowDefinition) -> WorkflowValidationResult:
        """Validate ``workflow`` and return its :class:`WorkflowValidationResult`.

        Raises
        ------
        InvalidWorkflowDefinitionError
            If workflow-level fields or metadata are invalid.
        InvalidStepDefinitionError
            If step-level fields, parameters, or duplicate step IDs are invalid.
        InvalidWorkflowDependencyError
            If a step references an unknown dependency, depends on itself, or
            declares duplicate dependencies.
        CyclicDependencyError
            If a cycle is present in the step dependency graph.
        """
        if not isinstance(workflow, WorkflowDefinition):
            raise InvalidWorkflowDefinitionError(
                f"Expected WorkflowDefinition instance, got {type(workflow).__name__}."
            )

        wf_id = workflow.workflow_id.strip() if isinstance(workflow.workflow_id, str) else ""
        wf_name = workflow.name.strip() if isinstance(workflow.name, str) else ""
        if not wf_id:
            raise InvalidWorkflowDefinitionError("Workflow 'workflow_id' must not be empty.")
        if not wf_name:
            raise InvalidWorkflowDefinitionError(
                "Workflow 'name' must not be empty.",
                workflow_id=wf_id,
            )
        if isinstance(workflow.version, bool) or workflow.version < 1:
            raise InvalidWorkflowDefinitionError(
                f"Workflow version must be >= 1, got {workflow.version!r}.",
                workflow_id=wf_id,
            )
        if not workflow.steps:
            raise InvalidWorkflowDefinitionError(
                f"Workflow '{wf_id}' must contain at least one step.",
                workflow_id=wf_id,
                workflow_version=workflow.version,
            )

        validate_json_compatible(
            workflow.metadata,
            path=f"workflow({wf_id}).metadata",
            error_cls=InvalidWorkflowDefinitionError,
        )

        step_ids_in_order: list[str] = []
        known_steps: set[str] = set()

        for step in workflow.steps:
            s_id = step.step_id.strip() if isinstance(step.step_id, str) else ""
            s_target = step.module_target.strip() if isinstance(step.module_target, str) else ""
            s_action = step.action.strip() if isinstance(step.action, str) else ""
            if not s_id or not s_target or not s_action:
                raise InvalidStepDefinitionError(
                    "Step 'step_id', 'module_target', and 'action' must be non-empty.",
                    workflow_id=wf_id,
                    workflow_version=workflow.version,
                    step_id=s_id or None,
                )
            if s_id in known_steps:
                raise InvalidStepDefinitionError(
                    f"Duplicate step_id '{s_id}' in workflow '{wf_id}'.",
                    workflow_id=wf_id,
                    workflow_version=workflow.version,
                    step_id=s_id,
                )
            validate_json_compatible(
                step.parameters,
                path=f"step({s_id}).parameters",
                error_cls=InvalidStepDefinitionError,
            )
            known_steps.add(s_id)
            step_ids_in_order.append(s_id)

        # Dependency validation & adjacency construction (prerequisite -> dependents)
        dependents: dict[str, list[str]] = {s_id: [] for s_id in step_ids_in_order}
        dependencies: dict[str, list[str]] = {s_id: [] for s_id in step_ids_in_order}
        in_degree: dict[str, int] = {s_id: 0 for s_id in step_ids_in_order}

        for step in workflow.steps:
            s_id = step.step_id
            seen_deps: set[str] = set()
            for dep in step.depends_on:
                dep_id = dep.strip() if isinstance(dep, str) else ""
                if not dep_id:
                    raise InvalidStepDefinitionError(
                        f"Step '{s_id}' contains an empty dependency identifier.",
                        workflow_id=wf_id,
                        workflow_version=workflow.version,
                        step_id=s_id,
                    )
                if dep_id == s_id:
                    raise InvalidWorkflowDependencyError(
                        f"Step '{s_id}' cannot depend on itself.",
                        workflow_id=wf_id,
                        workflow_version=workflow.version,
                        step_id=s_id,
                    )
                if dep_id in seen_deps:
                    raise InvalidWorkflowDependencyError(
                        f"Step '{s_id}' declares duplicate dependency '{dep_id}'.",
                        workflow_id=wf_id,
                        workflow_version=workflow.version,
                        step_id=s_id,
                    )
                if dep_id not in known_steps:
                    raise InvalidWorkflowDependencyError(
                        f"Step '{s_id}' depends on unknown step '{dep_id}' in workflow '{wf_id}'.",
                        workflow_id=wf_id,
                        workflow_version=workflow.version,
                        step_id=s_id,
                    )
                seen_deps.add(dep_id)
                dependencies[s_id].append(dep_id)
                dependents[dep_id].append(s_id)
                in_degree[s_id] += 1

        # Cycle detection via 3-color DFS on dependency edges (prerequisite -> dependent)
        cycle_path = self._find_cycle(step_ids_in_order, dependents)
        if cycle_path is not None:
            formatted = " -> ".join(cycle_path)
            raise CyclicDependencyError(
                f"Cyclic dependency detected in workflow '{wf_id}': {formatted}.",
                cycle=cycle_path,
                workflow_id=wf_id,
                workflow_version=workflow.version,
            )

        # Deterministic topological sort (Kahn's algorithm preserving definition order)
        ready: deque[str] = deque(s_id for s_id in step_ids_in_order if in_degree[s_id] == 0)
        topo_order: list[str] = []

        while ready:
            current = ready.popleft()
            topo_order.append(current)
            for nxt in dependents[current]:
                in_degree[nxt] -= 1
                if in_degree[nxt] == 0:
                    ready.append(nxt)

        if len(topo_order) != len(step_ids_in_order):
            raise CyclicDependencyError(
                f"Cyclic dependency detected in workflow '{wf_id}'.",
                workflow_id=wf_id,
                workflow_version=workflow.version,
            )

        return WorkflowValidationResult(
            is_valid=True,
            workflow_id=wf_id,
            workflow_version=workflow.version,
            topological_order=tuple(topo_order),
        )

    def get_topological_order(self, workflow: WorkflowDefinition) -> tuple[str, ...]:
        """Validate ``workflow`` and return its deterministic topological step order."""
        return self.validate(workflow).topological_order

    @staticmethod
    def _find_cycle(
        step_ids: list[str],
        dependents: dict[str, list[str]],
    ) -> tuple[str, ...] | None:
        """Perform 3-color DFS to detect and reconstruct a cycle path if one exists."""
        white, gray, black = 0, 1, 2
        color: dict[str, int] = {s_id: white for s_id in step_ids}
        stack: list[str] = []
        stack_index: dict[str, int] = {}

        def dfs(node: str) -> tuple[str, ...] | None:
            color[node] = gray
            stack_index[node] = len(stack)
            stack.append(node)

            for neighbor in dependents[node]:
                if color[neighbor] == gray:
                    start_idx = stack_index[neighbor]
                    return tuple([*stack[start_idx:], neighbor])
                if color[neighbor] == white:
                    found = dfs(neighbor)
                    if found is not None:
                        return found

            stack.pop()
            del stack_index[node]
            color[node] = black
            return None

        for step_id in step_ids:
            if color[step_id] == white:
                cycle = dfs(step_id)
                if cycle is not None:
                    return cycle
        return None


__all__ = [
    "WorkflowValidationResult",
    "WorkflowValidator",
]
