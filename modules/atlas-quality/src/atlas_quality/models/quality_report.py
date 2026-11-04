"""Immutable quality assessment report and request models."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from atlas_core import Artifact, DatasetId, generate_execution_id, require_not_empty
from atlas_storage import StorageLocation
from pydantic import BaseModel, ConfigDict, Field, model_validator

from atlas_quality.exceptions import InvalidQualityRequestError
from atlas_quality.models.quality_result import QualityResult, RuleOutcomeStatus
from atlas_quality.models.quality_rule import (
    QualityRuleCategory,
    QualityRuleConfig,
    QualityRuleSeverity,
)


class QualityAssessmentStatus(StrEnum):
    """Overall status of a dataset quality assessment report."""

    PASSED = "PASSED"
    PASSED_WITH_WARNINGS = "PASSED_WITH_WARNINGS"
    FAILED = "FAILED"
    ERROR = "ERROR"


class QualitySummary(BaseModel):
    """Aggregated summary statistics across categories and severities for a report."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    by_category: dict[str, dict[str, int]] = Field(default_factory=dict)
    by_severity: dict[str, dict[str, int]] = Field(default_factory=dict)
    issue_count: int = 0
    warning_failures: int = 0
    blocking_failures: int = 0


class QualityReport(BaseModel):
    """Immutable snapshot of a complete dataset quality assessment."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    assessment_id: str = Field(default_factory=generate_execution_id)
    execution_id: str = Field(default_factory=generate_execution_id)
    dataset_id: DatasetId
    artifact_reference: Artifact | None = None
    assessment_timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    overall_status: QualityAssessmentStatus = QualityAssessmentStatus.PASSED
    total_rules: int = 0
    passed_rules: int = 0
    failed_rules: int = 0
    execution_errors: int = 0
    total_evaluated_records: int = 0
    total_failed_records: int = 0
    quality_score: float = 100.0
    results: tuple[QualityResult, ...] = ()
    summary: QualitySummary = Field(default_factory=QualitySummary)
    profile: dict[str, Any] | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _derive_report_metrics(self) -> QualityReport:
        try:
            require_not_empty(self.assessment_id.strip(), "assessment_id")
            require_not_empty(self.execution_id.strip(), "execution_id")
            require_not_empty(str(self.dataset_id).strip(), "dataset_id")
        except ValueError as exc:
            raise InvalidQualityRequestError(str(exc)) from exc

        results_tuple = tuple(self.results)
        active_results = [r for r in results_tuple if r.status != RuleOutcomeStatus.SKIPPED]

        total_rules = len(active_results)
        passed_rules = sum(1 for r in active_results if r.passed and not r.is_execution_error)
        exec_errors = sum(
            1 for r in active_results if r.is_execution_error or r.status == RuleOutcomeStatus.ERROR
        )
        failed_rules = sum(
            1
            for r in active_results
            if not r.passed and not r.is_execution_error and r.status == RuleOutcomeStatus.FAILED
        )

        max_evaluated = max((r.evaluated_records for r in active_results), default=0)
        total_evaluated = (
            self.total_evaluated_records
            if "total_evaluated_records" in self.model_fields_set
            else max_evaluated
        )
        total_failed = (
            self.total_failed_records
            if "total_failed_records" in self.model_fields_set
            else sum(r.failed_records for r in active_results)
        )

        # Build summary breakdown
        by_cat: dict[str, dict[str, int]] = {}
        by_sev: dict[str, dict[str, int]] = {}
        warning_failures = 0
        blocking_failures = 0
        total_issues = 0

        for r in active_results:
            cat_key = str(r.category)
            sev_key = str(r.severity)
            by_cat.setdefault(cat_key, {"total": 0, "passed": 0, "failed": 0, "errors": 0})
            by_sev.setdefault(sev_key, {"total": 0, "passed": 0, "failed": 0, "errors": 0})

            by_cat[cat_key]["total"] += 1
            by_sev[sev_key]["total"] += 1
            total_issues += len(r.issues)

            if r.is_execution_error or r.status == RuleOutcomeStatus.ERROR:
                by_cat[cat_key]["errors"] += 1
                by_sev[sev_key]["errors"] += 1
            elif r.passed:
                by_cat[cat_key]["passed"] += 1
                by_sev[sev_key]["passed"] += 1
            else:
                by_cat[cat_key]["failed"] += 1
                by_sev[sev_key]["failed"] += 1
                if r.severity in {QualityRuleSeverity.INFO, QualityRuleSeverity.WARNING}:
                    warning_failures += 1
                else:
                    blocking_failures += 1

        # Determine overall_status: execution errors NEVER silently appear successful
        if "overall_status" not in self.model_fields_set:
            if exec_errors > 0:
                resolved_status = QualityAssessmentStatus.ERROR
            elif blocking_failures > 0:
                resolved_status = QualityAssessmentStatus.FAILED
            elif warning_failures > 0:
                resolved_status = QualityAssessmentStatus.PASSED_WITH_WARNINGS
            else:
                resolved_status = QualityAssessmentStatus.PASSED
        else:
            resolved_status = self.overall_status
            if exec_errors > 0 and resolved_status in {
                QualityAssessmentStatus.PASSED,
                QualityAssessmentStatus.PASSED_WITH_WARNINGS,
            }:
                resolved_status = QualityAssessmentStatus.ERROR

        if "quality_score" not in self.model_fields_set:
            if total_rules == 0:
                score = 100.0 if resolved_status != QualityAssessmentStatus.ERROR else 0.0
            else:
                avg_pct = sum(r.success_percentage for r in active_results) / total_rules
                score = round(avg_pct, 2)
        else:
            score = self.quality_score

        if "total_rules" not in self.model_fields_set:
            object.__setattr__(self, "total_rules", total_rules)
        if "passed_rules" not in self.model_fields_set:
            object.__setattr__(self, "passed_rules", passed_rules)
        if "failed_rules" not in self.model_fields_set:
            object.__setattr__(self, "failed_rules", failed_rules)
        if "execution_errors" not in self.model_fields_set:
            object.__setattr__(self, "execution_errors", exec_errors)

        object.__setattr__(self, "total_evaluated_records", total_evaluated)
        object.__setattr__(self, "total_failed_records", total_failed)
        object.__setattr__(self, "overall_status", resolved_status)
        object.__setattr__(self, "quality_score", score)

        if "summary" not in self.model_fields_set:
            object.__setattr__(
                self,
                "summary",
                QualitySummary(
                    by_category=by_cat,
                    by_severity=by_sev,
                    issue_count=total_issues,
                    warning_failures=warning_failures,
                    blocking_failures=blocking_failures,
                ),
            )

        return self


class QualityAssessmentRequest(BaseModel):
    """Strongly typed specification for executing a dataset quality assessment."""

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    assessment_id: str = Field(default_factory=generate_execution_id)
    dataset_id: DatasetId
    dataset: Any = None
    storage_location: StorageLocation | None = None
    artifact: Artifact | None = None
    reference_datasets: dict[str, Any] = Field(default_factory=dict)
    rules: list[QualityRuleConfig | dict[str, Any]] = Field(default_factory=list)
    enable_profiling: bool = False
    fail_fast: bool = False
    expose_samples: bool = False
    sample_limit: int = 5
    raise_on_error: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)

    def __init__(self, **data: Any) -> None:
        try:
            super().__init__(**data)
        except InvalidQualityRequestError:
            raise
        except Exception as exc:
            raise InvalidQualityRequestError(f"Invalid quality assessment request: {exc}") from exc

    @model_validator(mode="after")
    def _validate_request(self) -> QualityAssessmentRequest:
        cleaned_assessment_id = (
            self.assessment_id.strip() if isinstance(self.assessment_id, str) else ""
        )
        cleaned_dataset_id = str(self.dataset_id).strip() if self.dataset_id is not None else ""
        try:
            require_not_empty(cleaned_assessment_id, "assessment_id")
            require_not_empty(cleaned_dataset_id, "dataset_id")
        except ValueError as exc:
            raise InvalidQualityRequestError(str(exc)) from exc

        self.assessment_id = cleaned_assessment_id
        self.dataset_id = DatasetId(cleaned_dataset_id)

        if self.dataset is None and self.storage_location is None and self.artifact is None:
            raise InvalidQualityRequestError(
                "QualityAssessmentRequest requires at least one dataset input: "
                "'dataset', 'storage_location', or 'artifact'.",
                assessment_id=self.assessment_id,
            )

        if self.sample_limit < 0:
            raise InvalidQualityRequestError(
                f"sample_limit must be >= 0, got {self.sample_limit}.",
                assessment_id=self.assessment_id,
            )

        parsed_rules: list[QualityRuleConfig] = []
        for item in self.rules:
            if isinstance(item, QualityRuleConfig):
                rule_cfg = item
            elif isinstance(item, dict):
                merged = dict(item)
                merged.setdefault("expose_samples", self.expose_samples)
                merged.setdefault("sample_limit", self.sample_limit)
                rule_cfg = QualityRuleConfig(**merged)
            else:
                raise InvalidQualityRequestError(
                    f"Invalid rule configuration in request: {item!r}.",
                    assessment_id=self.assessment_id,
                )
            parsed_rules.append(rule_cfg)
        self.rules = parsed_rules  # type: ignore[assignment]
        return self


__all__ = [
    "QualityAssessmentRequest",
    "QualityAssessmentStatus",
    "QualityReport",
    "QualityRuleCategory",
    "QualitySummary",
]
