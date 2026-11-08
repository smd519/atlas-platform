"""Exceptions for the atlas-scheduler control-plane service."""

from __future__ import annotations

from atlas_core import AtlasError, ConfigurationError, ValidationError


class SchedulerError(AtlasError):
    """Base exception for all atlas-scheduler errors."""


class SchedulerConfigurationError(SchedulerError, ConfigurationError):
    """Raised when scheduler CLI or workspace configuration is invalid or missing."""


class WorkspaceIsolationError(SchedulerConfigurationError):
    """Raised when a workspace reference attempts path traversal or escapes ATLAS_WORKSPACE."""

    def __init__(
        self,
        message: str,
        *,
        offending_path: str | None = None,
        workspace_root: str | None = None,
    ) -> None:
        super().__init__(message)
        self.offending_path = offending_path
        self.workspace_root = workspace_root


class ScheduleValidationError(SchedulerError, ValidationError):
    """Raised when a schedule or trigger definition fails domain validation."""


__all__ = [
    "ScheduleValidationError",
    "SchedulerConfigurationError",
    "SchedulerError",
    "WorkspaceIsolationError",
]
