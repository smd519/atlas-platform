"""WorkflowResult standardized outcome model."""

from __future__ import annotations

from typing import Any

from atlas_core import ExecutionStatus, Result, require_not_empty
from pydantic import ConfigDict, Field, model_validator

from atlas_orchestration.exceptions import InvalidWorkflowStateError
from atlas_orchestration.models.step_execution import StepExecution


class WorkflowResult(Result):
    """Standardized outcome of a workflow execution.

    Extends :class:`atlas_core.Result` (`success`, `message`) with workflow
    `execution_id`, `status`, `step_results`, and `duration`.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    success: bool = False
    message: str | None = None
    execution_id: str
    workflow_id: str | None = None
    status: ExecutionStatus
    step_results: list[StepExecution] = Field(default_factory=list)
    duration: float = 0.0

    def __init__(self, **data: Any) -> None:
        if "success" not in data and "status" in data:
            data = dict(data)
            data["success"] = data["status"] == ExecutionStatus.SUCCEEDED
        try:
            super().__init__(**data)
        except InvalidWorkflowStateError:
            raise
        except Exception as exc:
            raise InvalidWorkflowStateError(f"Invalid workflow result: {exc}") from exc

    @model_validator(mode="after")
    def _validate_workflow_result(self) -> WorkflowResult:
        clean_exec_id = self.execution_id.strip() if isinstance(self.execution_id, str) else ""
        try:
            require_not_empty(clean_exec_id, "execution_id")
        except ValueError as exc:
            raise InvalidWorkflowStateError(str(exc)) from exc

        if isinstance(self.duration, bool) or self.duration < 0.0:
            raise InvalidWorkflowStateError(
                f"WorkflowResult duration must be >= 0.0, got {self.duration!r}.",
                execution_id=clean_exec_id,
            )

        for step_res in self.step_results:
            if step_res.execution_id != clean_exec_id:
                raise InvalidWorkflowStateError(
                    f"StepExecution '{step_res.step_execution_id}' belongs to execution_id "
                    f"'{step_res.execution_id}', expected '{clean_exec_id}'.",
                    execution_id=clean_exec_id,
                    step_id=step_res.step_id,
                    step_execution_id=step_res.step_execution_id,
                )

        object.__setattr__(self, "execution_id", clean_exec_id)
        object.__setattr__(self, "success", self.status == ExecutionStatus.SUCCEEDED)
        object.__setattr__(
            self,
            "step_results",
            [s.model_copy(deep=True) for s in self.step_results],
        )
        return self


__all__ = ["WorkflowResult"]
