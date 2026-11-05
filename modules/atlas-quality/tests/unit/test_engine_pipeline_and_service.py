"""Unit tests for QualityEngine, QualityPipeline, and QualityService."""

from __future__ import annotations

from typing import Any

import pytest
from atlas_core import DatasetId, ExecutionManager, ExecutionStatus
from atlas_quality import (
    ExecutionTracker,
    QualityAssessmentRequest,
    QualityAssessmentStatus,
    QualityEngine,
    QualityPipeline,
    QualityPipelineError,
    QualityRuleCategory,
    QualityRuleConfig,
    QualityRuleConfigurationError,
    QualityRuleSeverity,
    QualityService,
    RuleOutcomeStatus,
)
from atlas_storage import (
    FileSystemBackend,
    StorageLocation,
    StorageManager,
    StorageReadError,
)


class FailingExecutionManager(ExecutionManager):
    """Delegate ExecutionManager that fails on fail() to test tracking error resilience."""

    def start(self, execution_id: str) -> None:
        pass

    def complete(self, execution_id: str) -> None:
        pass

    def fail(self, execution_id: str, reason: str) -> None:
        raise RuntimeError("Database connection lost while recording failure")


class StubStorageManager(StorageManager):
    """Stub StorageManager for unit testing pipeline storage failures."""

    def __init__(self, data: Any = None, error: Exception | None = None) -> None:
        super().__init__(backend=FileSystemBackend())
        self._data = data
        self._error = error

    def load(self, location: StorageLocation) -> Any:
        if self._error is not None:
            raise self._error
        return self._data


def test_engine_executes_multiple_rules_and_isolates_rule_errors() -> None:
    """QualityEngine runs all enabled rules and does not discard results when one rule errors."""
    engine = QualityEngine()
    dataset = [
        {"id": 1, "email": "a@example.com"},
        {"id": 2, "email": "b@example.com"},
    ]
    rules = [
        QualityRuleConfig(
            rule_id="completeness.not_null",
            params={"column": "id"},
        ),
        QualityRuleConfig(
            rule_id="unknown.rule_id",
            params={},
        ),
        QualityRuleConfig(
            rule_id="uniqueness.unique_column",
            params={"column": "id"},
        ),
        QualityRuleConfig(
            rule_id="completeness.not_null",
            enabled=False,
            params={"column": "missing_col_but_disabled"},
        ),
    ]

    results, row_count = engine.execute(dataset, rules, fail_fast=False)
    assert row_count == 2
    assert len(results) == 3  # disabled rule skipped
    assert results[0].passed is True
    assert results[1].is_execution_error is True
    assert results[1].status == RuleOutcomeStatus.ERROR
    assert results[2].passed is True


def test_engine_fail_fast_stops_on_first_failure_or_error() -> None:
    """QualityEngine stops immediately when fail_fast=True."""
    engine = QualityEngine()
    dataset = [{"id": 1, "val": None}]
    rules = [
        QualityRuleConfig(
            rule_id="completeness.not_null",
            params={"column": "val"},
        ),
        QualityRuleConfig(
            rule_id="uniqueness.unique_column",
            params={"column": "id"},
        ),
    ]
    results, _ = engine.execute(dataset, rules, fail_fast=True)
    assert len(results) == 1
    assert results[0].passed is False

    with pytest.raises(QualityRuleConfigurationError):
        engine.execute(
            dataset,
            [
                QualityRuleConfig(
                    rule_id="completeness.not_null",
                    params={"column": "missing_col"},
                )
            ],
            fail_fast=True,
        )


def test_quality_service_and_pipeline_in_memory_assessment() -> None:
    """QualityService assesses in-memory datasets, profiles when requested, and stores reports."""
    service = QualityService()
    dataset = [
        {"order_id": "O-1", "amount": 50.0, "status": "PAID"},
        {"order_id": "O-2", "amount": 120.0, "status": "PAID"},
        {"order_id": "O-3", "amount": 80.0, "status": "PENDING"},
    ]

    report = service.assess_dataset(
        dataset,
        dataset_id="orders_silver",
        assessment_id="assess-orders-1",
        enable_profiling=True,
        rules=[
            {
                "rule_id": "schema.required_columns",
                "severity": "CRITICAL",
                "params": {"required_columns": ["order_id", "amount", "status"]},
            },
            {
                "rule_id": "uniqueness.unique_column",
                "severity": "ERROR",
                "params": {"column": "order_id"},
            },
            {
                "rule_id": "validity.allowed_values",
                "severity": "WARNING",
                "params": {"column": "status", "allowed_values": ["PAID"]},
            },
        ],
    )

    assert report.overall_status == QualityAssessmentStatus.PASSED_WITH_WARNINGS
    assert report.total_rules == 3
    assert report.passed_rules == 2
    assert report.failed_rules == 1
    assert report.profile is not None
    assert report.profile["row_count"] == 3
    assert report.summary.by_category[str(QualityRuleCategory.VALIDITY)]["failed"] == 1
    assert report.summary.by_severity[str(QualityRuleSeverity.WARNING)]["failed"] == 1

    # Retrieve report and execution via service
    fetched_report = service.get_report("assess-orders-1")
    assert fetched_report == report
    exec_rec = service.get_execution(report.execution_id)
    assert exec_rec.status == ExecutionStatus.SUCCEEDED
    assert len(service.list_executions(dataset_id="orders_silver")) == 1


def test_pipeline_storage_failure_and_tracking_resilience() -> None:
    """Pipeline wraps storage read errors and never masks original error when tracking fails."""
    stub_storage = StubStorageManager(error=StorageReadError("File corrupted"))
    tracker = ExecutionTracker(delegate=FailingExecutionManager())
    pipeline = QualityPipeline(storage_manager=stub_storage, tracker=tracker)

    request = QualityAssessmentRequest(
        assessment_id="assess-fail-1",
        dataset_id=DatasetId("corrupted_ds"),
        storage_location=StorageLocation(
            layer="bronze", path="/tmp/corrupted.parquet", format="parquet"
        ),
        rules=[
            QualityRuleConfig(
                rule_id="schema.required_columns",
                params={"required_columns": ["id"]},
            )
        ],
    )

    # Non-raising mode returns ERROR QualityReport with tracking_error recorded in metadata
    report = pipeline.run(request, raise_on_error=False)
    assert report.overall_status == QualityAssessmentStatus.ERROR
    assert report.execution_errors == 1
    assert "tracking_error" in report.metadata
    assert "File corrupted" in (report.results[0].error_message or "")

    # Raising mode surfaces QualityPipelineError (and does not mask it with tracking error)
    with pytest.raises(QualityPipelineError, match="Failed to load dataset") as exc_info:
        pipeline.run(request, raise_on_error=True)
    assert isinstance(exc_info.value.__cause__, StorageReadError)
