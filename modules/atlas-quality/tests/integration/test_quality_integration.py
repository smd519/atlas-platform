"""Integration tests for atlas-quality with atlas-storage and FileSystemBackend."""

from __future__ import annotations

from pathlib import Path

import pytest
from atlas_core import Artifact, DatasetId, ExecutionStatus
from atlas_quality import (
    QualityAssessmentRequest,
    QualityAssessmentState,
    QualityAssessmentStatus,
    QualityPipelineError,
    QualityRuleConfig,
    QualityRuleSeverity,
    QualityService,
)
from atlas_storage import FileSystemBackend, StorageLocation, StorageManager


@pytest.mark.parametrize(
    ("layer", "fmt"),
    [
        ("bronze", "csv"),
        ("silver", "json"),
        ("gold", "parquet"),
    ],
)
def test_assess_datasets_across_storage_formats_and_layers(
    tmp_path: Path,
    layer: str,
    fmt: str,
) -> None:
    """Verify assessing CSV, JSON, and Parquet datasets across bronze, silver, and gold layers."""
    storage_manager = StorageManager(backend=FileSystemBackend())
    service = QualityService(storage_manager=storage_manager)

    records = [
        {"order_id": "ORD-100", "customer_id": "CUST-1", "amount": 50.0, "total": 55.0, "tax": 5.0},
        {
            "order_id": "ORD-101",
            "customer_id": "CUST-2",
            "amount": 100.0,
            "total": 110.0,
            "tax": 10.0,
        },
        {"order_id": "ORD-102", "customer_id": "CUST-1", "amount": 25.0, "total": 27.5, "tax": 2.5},
    ]
    loc = StorageLocation(
        layer=layer,
        path=str(tmp_path / layer / f"orders.{fmt}"),
        format=fmt,
    )
    storage_manager.save(records, loc)

    ref_customers = [
        {"customer_id": "CUST-1"},
        {"customer_id": "CUST-2"},
    ]
    ref_loc = StorageLocation(
        layer="silver",
        path=str(tmp_path / "silver" / f"customers.{fmt}"),
        format=fmt,
    )
    storage_manager.save(ref_customers, ref_loc)

    request = QualityAssessmentRequest(
        dataset_id=DatasetId(f"orders_{layer}_{fmt}"),
        storage_location=loc,
        reference_datasets={"customers": ref_loc},
        enable_profiling=True,
        rules=[
            QualityRuleConfig(
                rule_id="schema.required_columns",
                severity=QualityRuleSeverity.CRITICAL,
                params={"required_columns": ["order_id", "customer_id", "amount", "tax", "total"]},
            ),
            QualityRuleConfig(
                rule_id="completeness.not_null",
                severity=QualityRuleSeverity.ERROR,
                params={"column": "order_id"},
            ),
            QualityRuleConfig(
                rule_id="uniqueness.unique_column",
                severity=QualityRuleSeverity.ERROR,
                params={"column": "order_id"},
            ),
            QualityRuleConfig(
                rule_id="validity.numeric_range",
                severity=QualityRuleSeverity.ERROR,
                params={"column": "amount", "min_value": 0.0, "max_value": 1000.0},
            ),
            QualityRuleConfig(
                rule_id="consistency.expression",
                severity=QualityRuleSeverity.ERROR,
                params={
                    "operation": "add",
                    "source_columns": ["amount", "tax"],
                    "target_column": "total",
                },
            ),
            QualityRuleConfig(
                rule_id="referential_integrity.foreign_key",
                severity=QualityRuleSeverity.ERROR,
                params={
                    "column": "customer_id",
                    "reference_dataset": "customers",
                    "reference_column": "customer_id",
                },
            ),
        ],
    )

    report = service.assess(request)
    assert report.overall_status == QualityAssessmentStatus.PASSED
    assert report.quality_score == 100.0
    assert report.total_rules == 6
    assert report.passed_rules == 6
    assert report.failed_rules == 0
    assert report.execution_errors == 0
    assert report.artifact_reference is not None
    assert report.artifact_reference.path == loc.path
    assert report.profile is not None
    assert report.profile["row_count"] == 3

    exec_rec = service.get_execution(report.execution_id)
    assert exec_rec.status == ExecutionStatus.SUCCEEDED
    assert exec_rec.state_history == [
        QualityAssessmentState.PENDING,
        QualityAssessmentState.LOADING,
        QualityAssessmentState.EVALUATING,
        QualityAssessmentState.PROFILING,
        QualityAssessmentState.REPORTING,
        QualityAssessmentState.COMPLETED,
    ]


def test_integration_missing_artifact_and_failed_rules(tmp_path: Path) -> None:
    """Verify missing artifact error handling and multi-rule failure reporting."""
    storage_manager = StorageManager(backend=FileSystemBackend())
    service = QualityService(storage_manager=storage_manager)

    missing_artifact = Artifact(
        id="art-missing",
        dataset_id="missing_ds",
        path=str(tmp_path / "bronze" / "does_not_exist.csv"),
        format="csv",
    )
    missing_req = QualityAssessmentRequest(
        dataset_id=DatasetId("missing_ds"),
        artifact=missing_artifact,
        rules=[
            QualityRuleConfig(
                rule_id="schema.required_columns",
                params={"required_columns": ["id"]},
            )
        ],
    )

    err_report = service.assess(missing_req)
    assert err_report.overall_status == QualityAssessmentStatus.ERROR
    assert err_report.execution_errors == 1
    assert service.get_execution(err_report.execution_id).status == ExecutionStatus.FAILED

    with pytest.raises(QualityPipelineError):
        service.assess(missing_req, raise_on_error=True)
