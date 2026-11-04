"""Ingestion-specific exception hierarchy."""

from __future__ import annotations

from atlas_core.exceptions import IngestionError as CoreIngestionError


class IngestionError(CoreIngestionError):
    """Base exception for all failures in the atlas-ingestion module."""

    def __init__(
        self,
        message: str,
        *,
        request_id: str | None = None,
        execution_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.request_id = request_id
        self.execution_id = execution_id


class InvalidIngestionRequestError(IngestionError, ValueError):
    """Raised when an ingestion request has missing or invalid configuration."""


class LifecycleTransitionError(IngestionError):
    """Raised when an invalid ingestion lifecycle state transition is attempted."""


class ExecutionTrackingError(IngestionError):
    """Raised when creating or updating an execution tracking record fails."""


class ExtractionError(IngestionError):
    """Raised when source resolution, connection validation, or data extraction fails."""


class LoadingError(IngestionError):
    """Raised when persisting extracted data into raw storage fails."""


__all__ = [
    "ExecutionTrackingError",
    "ExtractionError",
    "IngestionError",
    "InvalidIngestionRequestError",
    "LifecycleTransitionError",
    "LoadingError",
]
