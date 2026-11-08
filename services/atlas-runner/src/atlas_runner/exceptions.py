"""Domain, configuration, and workspace isolation exceptions for atlas-runner."""

from __future__ import annotations

from atlas_core import AtlasError


class RunnerError(AtlasError):
    """Base exception for all atlas-runner errors."""


class RunnerConfigurationError(RunnerError):
    """Raised when job or workflow YAML configuration cannot be parsed or validated."""

    def __init__(
        self,
        message: str,
        *,
        config_file: str | None = None,
        field: str | None = None,
    ) -> None:
        super().__init__(message)
        self.config_file = config_file
        self.field = field


class WorkspaceIsolationError(RunnerConfigurationError):
    """Raised when a resolved configuration or data path escapes the customer workspace."""

    def __init__(
        self,
        message: str,
        *,
        config_file: str | None = None,
        field: str | None = None,
        offending_path: str | None = None,
        workspace: str | None = None,
    ) -> None:
        super().__init__(message, config_file=config_file, field=field)
        self.offending_path = offending_path
        self.workspace = workspace


class RunnerExecutionError(RunnerError):
    """Raised when a workflow execution fails."""


__all__ = [
    "RunnerConfigurationError",
    "RunnerError",
    "RunnerExecutionError",
    "WorkspaceIsolationError",
]
