"""StepExecution runtime state model."""

from __future__ import annotations

import copy
from datetime import datetime
from typing import Any

from atlas_core import (
    ExecutionStatus,
    generate_execution_id,
    require_not_empty,
)
from pydantic import BaseModel, ConfigDict, Field, model_validator

from atlas_orchestration.exceptions import InvalidWorkflowStateError
from atlas_orchestration.models.workflow_step import (
    ensure_timezone_aware,
    validate_json_compatible,
)


class StepExecution(BaseModel):
    """Represents the execution state and outcome of an individual workflow step."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    step_execution_id: str = Field(default_factory=generate_execution_id)
    execution_id: str
    step_id: str
    module_target: str
    status: ExecutionStatus = ExecutionStatus.PENDING
    attempt_number: int = 1
    start_time: datetime | None = None
    end_time: datetime | None = None
    module_execution_ref: str | None = None
    error_code: str | None = None
    error_message: str | None = None
    output_references: dict[str, Any] = Field(default_factory=dict)

    def __init__(self, **data: Any) -> None:
        try:
            super().__init__(**data)
        except InvalidWorkflowStateError:
            raise
        except Exception as exc:
            raise InvalidWorkflowStateError(f"Invalid step execution record: {exc}") from exc

    @model_validator(mode="after")
    def _validate_step_execution(self) -> StepExecution:
        clean_step_exec_id = (
            self.step_execution_id.strip() if isinstance(self.step_execution_id, str) else ""
        )
        clean_exec_id = self.execution_id.strip() if isinstance(self.execution_id, str) else ""
        clean_step_id = self.step_id.strip() if isinstance(self.step_id, str) else ""
        clean_target = self.module_target.strip() if isinstance(self.module_target, str) else ""
        try:
            require_not_empty(clean_step_exec_id, "step_execution_id")
            require_not_empty(clean_exec_id, "execution_id")
            require_not_empty(clean_step_id, "step_id")
            require_not_empty(clean_target, "module_target")
        except ValueError as exc:
            raise InvalidWorkflowStateError(
                str(exc),
                execution_id=clean_exec_id or None,
                step_id=clean_step_id or None,
                step_execution_id=clean_step_exec_id or None,
            ) from exc

        if isinstance(self.attempt_number, bool) or self.attempt_number < 1:
            raise InvalidWorkflowStateError(
                f"attempt_number must be a positive integer >= 1, got {self.attempt_number!r}.",
                execution_id=clean_exec_id,
                step_id=clean_step_id,
                step_execution_id=clean_step_exec_id,
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
                f"StepExecution end_time ({end.isoformat()}) cannot precede "
                f"start_time ({start.isoformat()}).",
                execution_id=clean_exec_id,
                step_id=clean_step_id,
                step_execution_id=clean_step_exec_id,
            )

        if self.module_execution_ref is not None:
            clean_ref = self.module_execution_ref.strip()
            if not clean_ref:
                raise InvalidWorkflowStateError(
                    "module_execution_ref cannot be an empty string when provided.",
                    execution_id=clean_exec_id,
                    step_id=clean_step_id,
                    step_execution_id=clean_step_exec_id,
                )
            object.__setattr__(self, "module_execution_ref", clean_ref)

        try:
            validated_outputs = validate_json_compatible(
                self.output_references,
                path="output_references",
            )
        except Exception as exc:
            raise InvalidWorkflowStateError(
                f"Invalid output_references: {exc}",
                execution_id=clean_exec_id,
                step_id=clean_step_id,
                step_execution_id=clean_step_exec_id,
            ) from exc

        object.__setattr__(self, "step_execution_id", clean_step_exec_id)
        object.__setattr__(self, "execution_id", clean_exec_id)
        object.__setattr__(self, "step_id", clean_step_id)
        object.__setattr__(self, "module_target", clean_target)
        object.__setattr__(self, "start_time", start)
        object.__setattr__(self, "end_time", end)
        object.__setattr__(self, "output_references", copy.deepcopy(validated_outputs))
        return self


__all__ = ["StepExecution"]
