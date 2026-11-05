"""WorkflowExecution runtime state model."""

from __future__ import annotations

import copy
from datetime import datetime
from typing import Any

from atlas_core import (
    Execution,
    ExecutionStatus,
    generate_execution_id,
    require_not_empty,
)
from pydantic import ConfigDict, Field, model_validator

from atlas_orchestration.exceptions import InvalidWorkflowStateError
from atlas_orchestration.models.workflow_step import (
    ensure_timezone_aware,
    validate_json_compatible,
)


class WorkflowExecution(Execution):
    """Represents a runtime execution instance of a registered workflow.

    Extends :class:`atlas_core.Execution` (`id`, `status`, `started_at`,
    `completed_at`, `error_message`) while exposing orchestration-specific
    fields (`execution_id`, `workflow_id`, `workflow_version`, `start_time`,
    `end_time`, `context_overrides`).
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    id: str = ""
    execution_id: str = Field(default_factory=generate_execution_id)
    workflow_id: str
    workflow_version: int = 1
    status: ExecutionStatus = ExecutionStatus.PENDING
    start_time: datetime | None = None
    end_time: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    context_overrides: dict[str, Any] = Field(default_factory=dict)
    error_message: str | None = None

    def __init__(self, **data: Any) -> None:
        data = dict(data)
        if "execution_id" not in data and data.get("id"):
            data["execution_id"] = data["id"]
        if "start_time" not in data and data.get("started_at") is not None:
            data["start_time"] = data["started_at"]
        if "end_time" not in data and data.get("completed_at") is not None:
            data["end_time"] = data["completed_at"]

        try:
            super().__init__(**data)
        except InvalidWorkflowStateError:
            raise
        except Exception as exc:
            raise InvalidWorkflowStateError(f"Invalid workflow execution record: {exc}") from exc

    @model_validator(mode="after")
    def _validate_workflow_execution(self) -> WorkflowExecution:
        clean_exec_id = self.execution_id.strip() if isinstance(self.execution_id, str) else ""
        clean_wf_id = self.workflow_id.strip() if isinstance(self.workflow_id, str) else ""
        try:
            require_not_empty(clean_exec_id, "execution_id")
            require_not_empty(clean_wf_id, "workflow_id")
        except ValueError as exc:
            raise InvalidWorkflowStateError(
                str(exc),
                workflow_id=clean_wf_id or None,
                execution_id=clean_exec_id or None,
            ) from exc

        if isinstance(self.workflow_version, bool) or self.workflow_version < 1:
            raise InvalidWorkflowStateError(
                f"workflow_version must be >= 1, got {self.workflow_version!r}.",
                workflow_id=clean_wf_id,
                execution_id=clean_exec_id,
            )

        if self.status == ExecutionStatus.CANCELLED:
            raise InvalidWorkflowStateError(
                "WorkflowExecution status cannot be CANCELLED; valid workflow states are "
                "PENDING, RUNNING, SUCCEEDED, and FAILED.",
                workflow_id=clean_wf_id,
                execution_id=clean_exec_id,
            )

        start = ensure_timezone_aware(
            self.start_time,
            field_name="start_time",
            error_cls=InvalidWorkflowStateError,
        )
        end = ensure_timezone_aware(
            self.end_time,
            field_name="end_time",
            error_cls=InvalidWorkflowStateError,
        )

        if start is not None and end is not None and end < start:
            raise InvalidWorkflowStateError(
                f"WorkflowExecution end_time ({end.isoformat()}) cannot precede "
                f"start_time ({start.isoformat()}).",
                workflow_id=clean_wf_id,
                execution_id=clean_exec_id,
            )

        try:
            validated_overrides = validate_json_compatible(
                self.context_overrides,
                path="context_overrides",
            )
        except Exception as exc:
            raise InvalidWorkflowStateError(
                f"Invalid context_overrides: {exc}",
                workflow_id=clean_wf_id,
                execution_id=clean_exec_id,
            ) from exc

        object.__setattr__(self, "execution_id", clean_exec_id)
        object.__setattr__(self, "id", clean_exec_id)
        object.__setattr__(self, "workflow_id", clean_wf_id)
        object.__setattr__(self, "start_time", start)
        object.__setattr__(self, "end_time", end)
        object.__setattr__(self, "started_at", start)
        object.__setattr__(self, "completed_at", end)
        object.__setattr__(self, "context_overrides", copy.deepcopy(validated_overrides))
        return self


__all__ = ["WorkflowExecution"]
