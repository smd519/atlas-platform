"""Execution domain model."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from atlas_core.enums import ExecutionStatus


class Execution(BaseModel):
    """Represents the state and timing of a platform execution."""

    id: str
    status: ExecutionStatus = ExecutionStatus.PENDING
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error_message: str | None = None


__all__ = ["Execution"]
