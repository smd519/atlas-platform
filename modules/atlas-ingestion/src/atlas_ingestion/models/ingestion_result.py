"""Defines ingestion execution results and outcomes."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from atlas_core import Artifact, DatasetId, ExecutionStatus, Result, Source
from atlas_storage import StorageLocation
from pydantic import Field, model_validator

from atlas_ingestion.lifecycle import IngestionState


class IngestionResult(Result):
    """Standardized outcome of an ingestion pipeline execution.

    Extends :class:`atlas_core.models.result.Result` to avoid duplicating generic
    status and message fields.
    """

    success: bool = False
    message: str | None = None
    request_id: str
    execution_id: str
    status: ExecutionStatus
    lifecycle_state: IngestionState
    source: Source
    dataset_id: DatasetId
    destination: StorageLocation
    artifacts: list[Artifact] = Field(default_factory=list)
    row_count: int | None = None
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
    def _finalize_result(self) -> IngestionResult:
        expected_success = self.status == ExecutionStatus.SUCCEEDED
        self.success = expected_success

        if self.duration_seconds is None and self.completed_at is not None:
            delta = (self.completed_at - self.started_at).total_seconds()
            self.duration_seconds = max(0.0, delta)

        if not self.success:
            # Never report partial outputs as valid artifacts on a failed execution.
            self.artifacts = []
            if self.message is None and self.error_message is not None:
                self.message = self.error_message

        return self


__all__ = [
    "IngestionResult",
]
