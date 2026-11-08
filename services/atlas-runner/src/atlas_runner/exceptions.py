"""Domain and configuration exceptions for atlas-runner."""

from __future__ import annotations

from atlas_core import AtlasError


class RunnerError(AtlasError):
    """Base exception for all atlas-runner errors."""


class RunnerConfigurationError(RunnerError):
    """Raised when job or workflow YAML configuration cannot be parsed or validated."""


class RunnerExecutionError(RunnerError):
    """Raised when a workflow execution fails."""


__all__ = [
    "RunnerConfigurationError",
    "RunnerError",
    "RunnerExecutionError",
]
