"""JobResult domain model aggregating workflow execution outcomes for a Job."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from atlas_core import (
    ExecutionStatus,
    Result,
    generate_execution_id,
    require_not_empty,
)
from pydantic import ConfigDict, Field, model_validator

from atlas_orchestration.exceptions import InvalidWorkflowStateError
from atlas_orchestration.models.step_execution import StepExecution
from atlas_orchestration.models.workflow_result import WorkflowResult
from atlas_orchestration.models.workflow_step import ensure_timezone_aware


class JobResult(Result):
    """Standardized outcome of a batch or single-workflow Job execution.

    Aggregates one or more :class:`WorkflowResult` instances executed under a
    parent ``job_id`` and parent ``execution_id``.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    success: bool = False
    message: str | None = None
    job_id: str
    execution_id: str = Field(default_factory=generate_execution_id)
    status: ExecutionStatus
    workflow_results: list[WorkflowResult] = Field(default_factory=list)
    start_time: datetime | None = None
    end_time: datetime | None = None
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
            raise InvalidWorkflowStateError(f"Invalid job result: {exc}") from exc

    @model_validator(mode="after")
    def _validate_job_result(self) -> JobResult:
        clean_job_id = self.job_id.strip() if isinstance(self.job_id, str) else ""
        clean_exec_id = self.execution_id.strip() if isinstance(self.execution_id, str) else ""
        try:
            require_not_empty(clean_job_id, "job_id")
            require_not_empty(clean_exec_id, "execution_id")
        except ValueError as exc:
            raise InvalidWorkflowStateError(str(exc)) from exc

        if isinstance(self.duration, bool) or self.duration < 0.0:
            raise InvalidWorkflowStateError(
                f"JobResult duration must be >= 0.0, got {self.duration!r}.",
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
                f"JobResult end_time ({end.isoformat()}) cannot precede "
                f"start_time ({start.isoformat()}).",
                execution_id=clean_exec_id,
            )

        object.__setattr__(self, "job_id", clean_job_id)
        object.__setattr__(self, "execution_id", clean_exec_id)
        object.__setattr__(self, "start_time", start)
        object.__setattr__(self, "end_time", end)
        object.__setattr__(self, "success", self.status == ExecutionStatus.SUCCEEDED)
        object.__setattr__(
            self,
            "workflow_results",
            [wf.model_copy(deep=True) for wf in self.workflow_results],
        )
        return self

    @property
    def workflow_id(self) -> str | None:
        """Return the workflow_id when a single workflow was executed, or None otherwise."""
        if len(self.workflow_results) == 1:
            return self.workflow_results[0].workflow_id
        if len(self.workflow_results) > 1:
            ids = [wf.workflow_id for wf in self.workflow_results if wf.workflow_id]
            return ",".join(ids) if ids else None
        return None

    @property
    def step_results(self) -> list[StepExecution]:
        """Return aggregated step executions across all workflow results."""
        steps: list[StepExecution] = []
        for wf_res in self.workflow_results:
            steps.extend(wf_res.step_results)
        return steps


__all__ = ["JobResult"]
