"""Data quality assessment exception hierarchy for atlas-quality."""

from __future__ import annotations

from atlas_core import QualityError as CoreQualityError


class QualityError(CoreQualityError):
    """Base exception for all atlas-quality errors."""

    def __init__(
        self,
        message: str,
        *,
        assessment_id: str | None = None,
        execution_id: str | None = None,
        rule_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.assessment_id = assessment_id
        self.execution_id = execution_id
        self.rule_id = rule_id


class InvalidQualityRequestError(QualityError):
    """Raised when a quality assessment request or configuration is invalid."""


class UnknownQualityRuleError(QualityError):
    """Raised when a requested quality rule identifier is not registered."""


class DuplicateQualityRuleError(QualityError):
    """Raised when attempting to register a duplicate quality rule identifier."""


class QualityRuleConfigurationError(QualityError):
    """Raised when a quality rule configuration or parameter set is invalid."""


class QualityRuleExecutionError(QualityError):
    """Raised when a quality rule encounters an unexpected runtime execution error."""


class QualityEngineError(QualityError):
    """Raised when the quality execution engine fails to initialize or run."""


class QualityPipelineError(QualityError):
    """Raised when the quality assessment pipeline fails (e.g., storage read failure)."""


class QualityProfilingError(QualityError):
    """Raised when dataset profiling fails."""


class QualityExecutionTrackingError(QualityError):
    """Raised when quality execution lifecycle or state tracking fails."""


__all__ = [
    "DuplicateQualityRuleError",
    "InvalidQualityRequestError",
    "QualityEngineError",
    "QualityError",
    "QualityExecutionTrackingError",
    "QualityPipelineError",
    "QualityProfilingError",
    "QualityRuleConfigurationError",
    "QualityRuleExecutionError",
    "UnknownQualityRuleError",
]

