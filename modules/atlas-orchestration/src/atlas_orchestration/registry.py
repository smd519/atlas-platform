"""Thread-safe versioned workflow registry for atlas-orchestration."""

from __future__ import annotations

import threading

from atlas_orchestration.exceptions import (
    InvalidWorkflowDefinitionError,
    WorkflowNotFoundError,
    WorkflowVersionConflictError,
)
from atlas_orchestration.models.workflow import WorkflowDefinition
from atlas_orchestration.validation.workflow_validator import WorkflowValidator


class WorkflowRegistry:
    """Thread-safe in-memory registry for versioned :class:`WorkflowDefinition` objects."""

    def __init__(self, validator: WorkflowValidator | None = None) -> None:
        self._validator = validator or WorkflowValidator()
        self._lock = threading.RLock()
        self._workflows: dict[str, dict[int, WorkflowDefinition]] = {}

    @property
    def validator(self) -> WorkflowValidator:
        """Return the :class:`WorkflowValidator` used by this registry."""
        return self._validator

    def register(self, workflow: WorkflowDefinition) -> WorkflowDefinition:
        """Validate and atomically register ``workflow`` under ``(workflow_id, version)``.

        Raises
        ------
        InvalidWorkflowDefinitionError
            If ``workflow`` fails validation.
        WorkflowVersionConflictError
            If ``(workflow.workflow_id, workflow.version)`` is already registered.
        """
        self._validator.validate(workflow)
        snapshot = workflow.model_copy(deep=True)

        with self._lock:
            versions = self._workflows.setdefault(snapshot.workflow_id, {})
            if snapshot.version in versions:
                raise WorkflowVersionConflictError(
                    f"Workflow '{snapshot.workflow_id}' version {snapshot.version} "
                    "is already registered.",
                    workflow_id=snapshot.workflow_id,
                    workflow_version=snapshot.version,
                )
            versions[snapshot.version] = snapshot
            return snapshot.model_copy(deep=True)

    def get(self, workflow_id: str, version: int | None = None) -> WorkflowDefinition:
        """Retrieve a workflow by ``workflow_id`` and optional ``version``.

        When ``version`` is ``None``, the highest registered version is returned.

        Raises
        ------
        WorkflowNotFoundError
            If ``workflow_id`` or the requested ``version`` is not registered.
        """
        clean_id = workflow_id.strip() if isinstance(workflow_id, str) else ""
        if not clean_id:
            raise WorkflowNotFoundError("Workflow identifier must not be empty.")

        if version is not None and (isinstance(version, bool) or version < 1):
            raise InvalidWorkflowDefinitionError(
                f"Workflow version must be an integer >= 1, got {version!r}.",
                workflow_id=clean_id,
            )

        with self._lock:
            versions = self._workflows.get(clean_id)
            if not versions:
                raise WorkflowNotFoundError(
                    f"Workflow '{clean_id}' is not registered.",
                    workflow_id=clean_id,
                    workflow_version=version,
                )
            if version is None:
                latest_ver = max(versions.keys())
                return versions[latest_ver].model_copy(deep=True)
            if version not in versions:
                raise WorkflowNotFoundError(
                    f"Workflow '{clean_id}' version {version} is not registered.",
                    workflow_id=clean_id,
                    workflow_version=version,
                )
            return versions[version].model_copy(deep=True)

    def get_latest(self, workflow_id: str) -> WorkflowDefinition:
        """Return the latest (highest) version registered for ``workflow_id``."""
        return self.get(workflow_id, version=None)

    def list_workflows(self, workflow_id: str | None = None) -> list[WorkflowDefinition]:
        """List registered workflow definitions ordered by ``(workflow_id, version)``.

        If ``workflow_id`` is provided, only versions of that workflow are returned.
        """
        with self._lock:
            if workflow_id is not None:
                clean_id = workflow_id.strip()
                versions = self._workflows.get(clean_id, {})
                return [versions[ver].model_copy(deep=True) for ver in sorted(versions.keys())]

            results: list[WorkflowDefinition] = []
            for wf_id in sorted(self._workflows.keys()):
                versions = self._workflows[wf_id]
                for ver in sorted(versions.keys()):
                    results.append(versions[ver].model_copy(deep=True))
            return results

    def unregister(self, workflow_id: str, version: int | None = None) -> None:
        """Remove a specific workflow version or all versions of ``workflow_id``.

        Raises
        ------
        WorkflowNotFoundError
            If the workflow or specific version does not exist in the registry.
        """
        clean_id = workflow_id.strip() if isinstance(workflow_id, str) else ""
        if not clean_id:
            raise WorkflowNotFoundError("Workflow identifier must not be empty.")

        with self._lock:
            versions = self._workflows.get(clean_id)
            if not versions:
                raise WorkflowNotFoundError(
                    f"Workflow '{clean_id}' is not registered.",
                    workflow_id=clean_id,
                    workflow_version=version,
                )
            if version is None:
                del self._workflows[clean_id]
                return

            if version not in versions:
                raise WorkflowNotFoundError(
                    f"Workflow '{clean_id}' version {version} is not registered.",
                    workflow_id=clean_id,
                    workflow_version=version,
                )
            del versions[version]
            if not versions:
                del self._workflows[clean_id]


__all__ = ["WorkflowRegistry"]
