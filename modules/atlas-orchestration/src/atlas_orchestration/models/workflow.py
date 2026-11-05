"""WorkflowDefinition domain model."""

from __future__ import annotations

import copy
from typing import Any

from atlas_core import require_not_empty
from pydantic import BaseModel, ConfigDict, Field, model_validator

from atlas_orchestration.exceptions import (
    InvalidStepDefinitionError,
    InvalidWorkflowDefinitionError,
)
from atlas_orchestration.models.workflow_step import (
    WorkflowStep,
    validate_json_compatible,
)


class WorkflowDefinition(BaseModel):
    """Immutable versioned workflow blueprint consisting of ordered/DAG steps."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    workflow_id: str
    name: str
    description: str | None = None
    version: int = 1
    steps: list[WorkflowStep] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    def __init__(self, **data: Any) -> None:
        raw_ver = data.get("version", 1)
        if isinstance(raw_ver, bool) or not isinstance(raw_ver, int) or raw_ver < 1:
            raise InvalidWorkflowDefinitionError(
                f"Workflow version must be an integer >= 1, got {raw_ver!r}.",
                workflow_id=str(data.get("workflow_id") or "") or None,
            )
        try:
            super().__init__(**data)
        except InvalidWorkflowDefinitionError:
            raise
        except Exception as exc:
            raise InvalidWorkflowDefinitionError(
                f"Invalid workflow definition: {exc}",
                workflow_id=str(data.get("workflow_id") or "") or None,
            ) from exc

    @model_validator(mode="after")
    def _validate_workflow(self) -> WorkflowDefinition:
        clean_id = self.workflow_id.strip() if isinstance(self.workflow_id, str) else ""
        clean_name = self.name.strip() if isinstance(self.name, str) else ""
        try:
            require_not_empty(clean_id, "workflow_id")
            require_not_empty(clean_name, "name")
        except ValueError as exc:
            raise InvalidWorkflowDefinitionError(
                str(exc),
                workflow_id=clean_id or None,
                workflow_version=self.version,
            ) from exc

        if self.version < 1:
            raise InvalidWorkflowDefinitionError(
                f"Workflow version must be >= 1, got {self.version}.",
                workflow_id=clean_id,
                workflow_version=self.version,
            )

        if not self.steps:
            raise InvalidWorkflowDefinitionError(
                f"Workflow '{clean_id}' must contain at least one WorkflowStep.",
                workflow_id=clean_id,
                workflow_version=self.version,
            )

        seen_steps: set[str] = set()
        for step in self.steps:
            if step.step_id in seen_steps:
                raise InvalidStepDefinitionError(
                    f"Duplicate step_id '{step.step_id}' in workflow '{clean_id}'.",
                    workflow_id=clean_id,
                    workflow_version=self.version,
                    step_id=step.step_id,
                )
            seen_steps.add(step.step_id)

        validated_metadata = validate_json_compatible(
            self.metadata,
            path=f"workflow({clean_id}).metadata",
            error_cls=InvalidWorkflowDefinitionError,
        )

        object.__setattr__(self, "workflow_id", clean_id)
        object.__setattr__(self, "name", clean_name)
        object.__setattr__(
            self,
            "steps",
            [step.model_copy(deep=True) for step in self.steps],
        )
        object.__setattr__(self, "metadata", copy.deepcopy(validated_metadata))
        return self

    def get_step(self, step_id: str) -> WorkflowStep | None:
        """Return the :class:`WorkflowStep` matching ``step_id``, or ``None``."""
        for step in self.steps:
            if step.step_id == step_id:
                return step.model_copy(deep=True)
        return None


__all__ = ["WorkflowDefinition"]

