"""Immutable outcome of a single quality rule execution."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from atlas_core import QualityResult as CoreQualityResult
from pydantic import ConfigDict, Field, model_validator

from atlas_quality.exceptions import QualityRuleConfigurationError
from atlas_quality.models.quality_issue import QualityIssue
from atlas_quality.models.quality_rule import QualityRuleCategory, QualityRuleSeverity


class RuleOutcomeStatus(StrEnum):
    """Discrete outcome status of a single rule execution."""

    PASSED = "PASSED"
    FAILED = "FAILED"
    ERROR = "ERROR"
    SKIPPED = "SKIPPED"


class QualityResult(CoreQualityResult):
    """Immutable assessment outcome for a single quality rule execution.

    Extends :class:`atlas_core.QualityResult` while distinguishing between a
    failed data quality check (`status == FAILED`) and a runtime rule execution
    error (`status == ERROR`, `is_execution_error == True`).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    rule_id: str
    rule_name: str = ""
    category: QualityRuleCategory = QualityRuleCategory.VALIDITY
    severity: QualityRuleSeverity = QualityRuleSeverity.ERROR
    passed: bool
    status: RuleOutcomeStatus = RuleOutcomeStatus.PASSED
    evaluated_records: int = 0
    failed_records: int = 0
    success_percentage: float = 100.0
    execution_duration_seconds: float = 0.0
    issues: tuple[QualityIssue, ...] = ()
    is_execution_error: bool = False
    error_type: str | None = None
    error_message: str | None = None
    error_cause: str | None = None
    message: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    def __init__(self, **data: Any) -> None:
        if not data.get("rule_name") and data.get("rule_id"):
            data["rule_name"] = str(data["rule_id"])
        super().__init__(**data)

    @model_validator(mode="after")
    def _validate_and_compute_metrics(self) -> QualityResult:
        if not self.rule_id or not self.rule_id.strip():
            raise QualityRuleConfigurationError("QualityResult 'rule_id' cannot be empty.")

        if self.evaluated_records < 0 or self.failed_records < 0:
            raise QualityRuleConfigurationError(
                "evaluated_records and failed_records must be non-negative."
            )
        if self.execution_duration_seconds < 0.0:
            raise QualityRuleConfigurationError("execution_duration_seconds must be non-negative.")

        if self.error_message is not None or self.error_type is not None or self.is_execution_error:
            object.__setattr__(self, "is_execution_error", True)
            object.__setattr__(self, "passed", False)
            object.__setattr__(self, "status", RuleOutcomeStatus.ERROR)
            object.__setattr__(self, "success_percentage", 0.0)
            if self.message is None and self.error_message is not None:
                object.__setattr__(self, "message", self.error_message)
            return self

        if "status" not in self.model_fields_set:
            resolved_status = RuleOutcomeStatus.PASSED if self.passed else RuleOutcomeStatus.FAILED
            object.__setattr__(self, "status", resolved_status)

        if "success_percentage" not in self.model_fields_set:
            if self.evaluated_records > 0:
                passed_records = max(0, self.evaluated_records - self.failed_records)
                pct = round((passed_records / self.evaluated_records) * 100.0, 4)
            else:
                pct = 100.0 if self.passed else 0.0
            object.__setattr__(self, "success_percentage", pct)

        if self.message is None:
            if self.passed:
                object.__setattr__(self, "message", f"Rule '{self.rule_name}' passed.")
            elif self.issues:
                object.__setattr__(self, "message", self.issues[0].description)
            else:
                object.__setattr__(self, "message", f"Rule '{self.rule_name}' failed.")

        return self


__all__ = [
    "QualityResult",
    "RuleOutcomeStatus",
]

