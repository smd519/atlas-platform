"""Defines standardized processing execution results and statistics."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from atlas_core import Artifact, DatasetId, ExecutionStatus, Result
from pydantic import BaseModel, Field, model_validator

from atlas_processing.tracking.execution_tracker import ProcessingState


class ProcessingStatistics(BaseModel):
    """Summary metrics recorded for a processing execution."""

    input_row_count: int = 0
    output_row_count: int = 0
    rows_delta: int = 0
    input_column_count: int = 0
    output_column_count: int = 0
    transformations_applied: int = 0
    duration_seconds: float | None = None

    @model_validator(mode="after")
    def _compute_delta(self) -> ProcessingStatistics:
        self.rows_delta = self.output_row_count - self.input_row_count
        return self


class ProcessingResult(Result):
    """Standardized outcome of a dataset processing pipeline execution.

    Extends :class:`atlas_core.Result` to avoid duplicating generic status and
    message fields.
    """

    success: bool = False
    message: str | None = None
    request_id: str
    execution_id: str
    dataset_id: DatasetId
    status: ExecutionStatus
    lifecycle_state: ProcessingState
    input_artifact: Artifact | None = None
    output_artifact: Artifact | None = None
    statistics: ProcessingStatistics = Field(default_factory=ProcessingStatistics)
    started_at: datetime
    completed_at: datetime | None = None
    duration_seconds: float | None = None
    error_type: str | None = None
    error_message: str | None = None
    error_cause: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    def __init__(self, **data: Any) -> None:
        if "success" not in data and "status" in data:
            data["success"] = data["status"] == ExecutionStatus.SUCCEEDED
        super().__init__(**data)

    @model_validator(mode="after")
    def _finalize_result(self) -> ProcessingResult:
        self.success = self.status == ExecutionStatus.SUCCEEDED

        if self.duration_seconds is None and self.completed_at is not None:
            delta = (self.completed_at - self.started_at).total_seconds()
            self.duration_seconds = max(0.0, delta)

        if self.statistics.duration_seconds is None and self.duration_seconds is not None:
            self.statistics.duration_seconds = self.duration_seconds

        if not self.success:
            # Never expose partial output artifacts on a failed processing execution.
            self.output_artifact = None
            if self.message is None and self.error_message is not None:
                self.message = self.error_message

        return self


__all__ = [
    "ProcessingResult",
    "ProcessingStatistics",
]

