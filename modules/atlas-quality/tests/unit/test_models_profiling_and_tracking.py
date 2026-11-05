"""Unit tests for quality models, profiling, lifecycle, and execution tracking."""

from __future__ import annotations

import pytest
from atlas_core import DatasetId, ExecutionManager, ExecutionStatus
from atlas_quality import (
    DatasetProfiler,
    ExecutionTracker,
    InvalidQualityRequestError,
    QualityAssessmentLifecycle,
    QualityAssessmentRequest,
    QualityAssessmentState,
    QualityAssessmentStatus,
    QualityExecutionTrackingError,
    QualityIssue,
    QualityProfilingError,
    QualityReport,
    QualityResult,
    QualityRuleCategory,
    QualityRuleConfig,
    QualityRuleConfigurationError,
    QualityRuleScope,
    QualityRuleSeverity,
    RuleOutcomeStatus,
)
from pydantic import ValidationError


class RecordingExecutionManager(ExecutionManager):
    """In-memory delegate ExecutionManager for unit testing."""

    def __init__(self) -> None:
        self.started: list[str] = []
        self.completed: list[str] = []
        self.failed: list[tuple[str, str]] = []

    def start(self, execution_id: str) -> None:
        self.started.append(execution_id)

    def complete(self, execution_id: str) -> None:
        self.completed.append(execution_id)

    def fail(self, execution_id: str, reason: str) -> None:
        self.failed.append((execution_id, reason))


def test_quality_rule_config_validation_and_immutability() -> None:
    """QualityRuleConfig normalizes enums, validates inputs, and is immutable."""
    cfg = QualityRuleConfig(
        rule_id="completeness.not_null",
        name="Customer ID Not Null",
        severity="warning",  # type: ignore[arg-type]
        category="completeness",  # type: ignore[arg-type]
        scope="column",  # type: ignore[arg-type]
        params={"column": "customer_id"},
        sample_limit=3,
        expose_samples=False,
    )
    assert cfg.severity == QualityRuleSeverity.WARNING
    assert cfg.category == QualityRuleCategory.COMPLETENESS
    assert cfg.scope == QualityRuleScope.COLUMN

    with pytest.raises(ValidationError):
        cfg.rule_id = "mutated"  # type: ignore[misc]

    with pytest.raises(QualityRuleConfigurationError, match="rule_id"):
        QualityRuleConfig(rule_id="   ")

    with pytest.raises(QualityRuleConfigurationError, match="severity"):
        QualityRuleConfig(rule_id="r1", severity="fatal")  # type: ignore[arg-type]

    with pytest.raises(QualityRuleConfigurationError, match="sample_limit"):
        QualityRuleConfig(rule_id="r1", sample_limit=-1)


def test_quality_issue_redacts_sensitive_samples_by_default() -> None:
    """QualityIssue hides sample_values unless expose_samples=True and enforces sample_limit."""
    default_issue = QualityIssue(
        rule_id="validity.regex",
        severity=QualityRuleSeverity.ERROR,
        category=QualityRuleCategory.VALIDITY,
        description="Invalid SSN format",
        affected_column="ssn",
        affected_record_count=4,
        record_references=(0, 1, 2, 3),
        sample_values=("111-11-1111", "222-22-2222", "333-33-3333", "444-44-4444"),
        sample_limit=2,
    )
    assert default_issue.expose_samples is False
    assert default_issue.sample_values == ()
    assert default_issue.record_references == (0, 1)

    exposed_issue = QualityIssue(
        rule_id="validity.regex",
        severity=QualityRuleSeverity.ERROR,
        category=QualityRuleCategory.VALIDITY,
        description="Invalid code",
        affected_column="code",
        affected_record_count=4,
        record_references=(0, 1, 2, 3),
        sample_values=("A", "B", "C", "D"),
        expose_samples=True,
        sample_limit=2,
    )
    assert exposed_issue.sample_values == ("A", "B")


def test_quality_result_and_report_status_distinctions() -> None:
    """QualityReport distinguishes PASSED, PASSED_WITH_WARNINGS, FAILED, and ERROR."""
    pass_res = QualityResult(
        rule_id="r_pass",
        category=QualityRuleCategory.SCHEMA,
        severity=QualityRuleSeverity.CRITICAL,
        passed=True,
        evaluated_records=10,
        failed_records=0,
    )
    warn_fail_res = QualityResult(
        rule_id="r_warn",
        category=QualityRuleCategory.COMPLETENESS,
        severity=QualityRuleSeverity.WARNING,
        passed=False,
        evaluated_records=10,
        failed_records=2,
    )
    err_fail_res = QualityResult(
        rule_id="r_err",
        category=QualityRuleCategory.UNIQUENESS,
        severity=QualityRuleSeverity.ERROR,
        passed=False,
        evaluated_records=10,
        failed_records=1,
    )
    exec_err_res = QualityResult(
        rule_id="r_crash",
        category=QualityRuleCategory.VALIDITY,
        severity=QualityRuleSeverity.ERROR,
        passed=False,
        error_type="QualityRuleExecutionError",
        error_message="Missing reference dataset",
    )
    assert exec_err_res.is_execution_error is True
    assert exec_err_res.status == RuleOutcomeStatus.ERROR

    # 1. All passed -> PASSED
    rep_pass = QualityReport(
        dataset_id=DatasetId("ds"),
        results=(pass_res,),
    )
    assert rep_pass.overall_status == QualityAssessmentStatus.PASSED
    assert rep_pass.quality_score == 100.0

    # 2. Only WARNING failed -> PASSED_WITH_WARNINGS
    rep_warn = QualityReport(
        dataset_id=DatasetId("ds"),
        results=(pass_res, warn_fail_res),
    )
    assert rep_warn.overall_status == QualityAssessmentStatus.PASSED_WITH_WARNINGS
    assert rep_warn.passed_rules == 1
    assert rep_warn.failed_rules == 1
    assert rep_warn.execution_errors == 0
    assert rep_warn.quality_score == 90.0

    # 3. ERROR/CRITICAL rule failed -> FAILED
    rep_fail = QualityReport(
        dataset_id=DatasetId("ds"),
        results=(pass_res, warn_fail_res, err_fail_res),
    )
    assert rep_fail.overall_status == QualityAssessmentStatus.FAILED
    assert rep_fail.summary.blocking_failures == 1
    assert rep_fail.summary.warning_failures == 1

    # 4. Execution error present -> ERROR (never silently PASSED)
    rep_error = QualityReport(
        dataset_id=DatasetId("ds"),
        overall_status=QualityAssessmentStatus.PASSED,
        results=(pass_res, exec_err_res),
    )
    assert rep_error.overall_status == QualityAssessmentStatus.ERROR
    assert rep_error.execution_errors == 1


def test_assessment_request_validation() -> None:
    """QualityAssessmentRequest validates dataset inputs and rule configs."""
    with pytest.raises(InvalidQualityRequestError, match="requires at least one dataset input"):
        QualityAssessmentRequest(dataset_id=DatasetId("ds"))

    with pytest.raises(InvalidQualityRequestError, match="dataset_id"):
        QualityAssessmentRequest(dataset_id=DatasetId("   "), dataset=[{"a": 1}])


def test_profiler_statistics_and_large_dataset() -> None:
    """DatasetProfiler computes accurate column statistics and handles larger datasets."""
    profiler = DatasetProfiler()
    records = [
        {"id": 1, "amount": 10.0, "tag": "A"},
        {"id": 2, "amount": 20.0, "tag": "B"},
        {"id": 2, "amount": 20.0, "tag": "B"},
        {"id": 3, "amount": None, "tag": None},
    ]
    profile = profiler.profile(records)
    assert profile.row_count == 4
    assert profile.column_count == 3
    assert profile.duplicate_row_count == 1
    assert profile.columns["amount"].null_count == 1
    assert profile.columns["amount"].null_percentage == 25.0
    assert profile.columns["amount"].min_value == 10.0
    assert profile.columns["amount"].max_value == 20.0
    assert pytest.approx(profile.columns["amount"].mean_value) == 16.666667
    assert profile.columns["tag"].distinct_count == 2

    # Large dataset profiling (10,000 rows)
    large_data = {"id": list(range(10_000)), "value": [float(i % 100) for i in range(10_000)]}
    large_profile = profiler.profile(large_data)
    assert large_profile.row_count == 10_000
    assert large_profile.columns["id"].distinct_count == 10_000
    assert large_profile.columns["value"].min_value == 0.0
    assert large_profile.columns["value"].max_value == 99.0

    with pytest.raises(QualityProfilingError):
        profiler.profile(None)


def test_lifecycle_and_execution_tracker() -> None:
    """QualityAssessmentLifecycle and ExecutionTracker enforce state transitions and delegate."""
    lifecycle = QualityAssessmentLifecycle()
    assert lifecycle.state == QualityAssessmentState.PENDING
    assert not lifecycle.can_transition_to(QualityAssessmentState.COMPLETED)
    lifecycle.transition_to(QualityAssessmentState.EVALUATING)
    lifecycle.transition_to(QualityAssessmentState.REPORTING)
    lifecycle.transition_to(QualityAssessmentState.COMPLETED)
    assert lifecycle.is_terminal is True

    with pytest.raises(QualityExecutionTrackingError):
        lifecycle.transition_to(QualityAssessmentState.EVALUATING)

    delegate = RecordingExecutionManager()
    tracker = ExecutionTracker(delegate=delegate)
    rec = tracker.create_execution(
        assessment_id="assess-1",
        dataset_id="orders",
        execution_id="exec-1",
    )
    tracker.start(rec.id)
    tracker.record_transition(rec.id, QualityAssessmentState.EVALUATING)
    report = QualityReport(
        assessment_id="assess-1",
        execution_id="exec-1",
        dataset_id=DatasetId("orders"),
    )
    tracker.complete(rec.id, report=report)

    assert delegate.started == ["exec-1"]
    assert delegate.completed == ["exec-1"]
    assert tracker.get_report("assess-1") == report
    assert tracker.get_execution("exec-1").status == ExecutionStatus.SUCCEEDED
    assert len(tracker.list_executions(dataset_id="orders")) == 1
