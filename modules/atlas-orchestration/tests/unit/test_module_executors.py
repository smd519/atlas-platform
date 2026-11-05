"""Unit tests for concrete module executors (Ingestion, Processing, Quality)."""

from __future__ import annotations

from typing import Any

import pytest
from atlas_core import ExecutionStatus
from atlas_orchestration import (
    ExecutionContext,
    IngestionExecutor,
    OrchestrationDependencies,
    ProcessingExecutor,
    QualityExecutor,
    StepExecutionError,
    WorkflowStep,
)


class _StubIngestionService:
    def __init__(self, response: dict[str, Any] | None = None) -> None:
        self.last_request: Any = None
        self._response = response or {
            "execution_id": "ing_exec_01",
            "request_id": "ing_req_01",
            "dataset_id": "customers",
            "status": "succeeded",
            "artifact_path": "bronze/customers.parquet",
            "row_count": 500,
        }

    def ingest(self, request: Any) -> dict[str, Any]:
        self.last_request = request
        return self._response


class _StubProcessingService:
    def __init__(self, response: dict[str, Any] | None = None) -> None:
        self.last_request: Any = None
        self._response = response or {
            "execution_id": "proc_exec_01",
            "request_id": "proc_req_01",
            "dataset_id": "customers",
            "status": "succeeded",
            "artifact_path": "silver/customers.parquet",
            "output_row_count": 480,
        }

    def process(self, request: Any) -> dict[str, Any]:
        self.last_request = request
        return self._response


class _StubQualityService:
    def __init__(self, response: dict[str, Any] | None = None) -> None:
        self.last_request: Any = None
        self._response = response or {
            "assessment_id": "qual_ass_01",
            "execution_id": "qual_exec_01",
            "dataset_id": "customers",
            "overall_status": "PASSED",
            "quality_score": 100.0,
        }

    def assess(self, request: Any) -> dict[str, Any]:
        self.last_request = request
        return self._response


def test_ingestion_executor_invokes_injected_service() -> None:
    stub = _StubIngestionService()
    deps = OrchestrationDependencies(ingestion_service=stub)
    executor = IngestionExecutor()
    ctx = ExecutionContext(workflow_id="wf_mod", execution_id="exec_mod_1")

    step = WorkflowStep(
        step_id="ingest_customers",
        module_target="atlas-ingestion",
        action="ingest",
        parameters={
            "dataset_id": "customers",
            "source_format": "csv",
            "source": {
                "source_id": "src_cust",
                "source_name": "Customer CSV",
                "source_type": "local_file",
                "config": {"path": "raw/customers.csv"},
            },
            "destination": {
                "layer": "bronze",
                "path": "bronze/customers.parquet",
                "format": "parquet",
            },
        },
    )

    step_exec = executor.execute(step, ctx, deps)
    assert step_exec.status == ExecutionStatus.SUCCEEDED
    assert step_exec.module_execution_ref == "ing_exec_01"
    assert step_exec.output_references["artifact_path"] == "bronze/customers.parquet"
    # Executor returns outputs via StepExecution; WorkflowEngine owns writing to ctx.step_outputs
    assert ctx.has_step_output("ingest_customers") is False
    assert stub.last_request is not None


def test_processing_executor_invokes_injected_service() -> None:
    stub = _StubProcessingService()
    deps = OrchestrationDependencies(processing_service=stub)
    executor = ProcessingExecutor()
    ctx = ExecutionContext(workflow_id="wf_mod", execution_id="exec_mod_2")

    step = WorkflowStep(
        step_id="transform_customers",
        module_target="atlas-processing",
        action="transform",
        parameters={
            "dataset_id": "customers",
            "input_path": "bronze/customers.parquet",
            "output_path": "silver/customers.parquet",
            "transformations": [
                {
                    "transformation_id": "filter",
                    "params": {"column": "id", "operator": "not_null"},
                }
            ],
        },
    )

    step_exec = executor.execute(step, ctx, deps)
    assert step_exec.status == ExecutionStatus.SUCCEEDED
    assert step_exec.module_execution_ref == "proc_exec_01"
    assert step_exec.output_references["artifact_path"] == "silver/customers.parquet"
    assert ctx.has_step_output("transform_customers") is False


def test_quality_executor_invokes_injected_service_and_detects_failures() -> None:
    stub = _StubQualityService()
    deps = OrchestrationDependencies(quality_service=stub)
    executor = QualityExecutor()
    ctx = ExecutionContext(workflow_id="wf_mod", execution_id="exec_mod_3")

    step = WorkflowStep(
        step_id="check_customers",
        module_target="atlas-quality",
        action="assess",
        parameters={
            "dataset_id": "customers",
            "input_path": "silver/customers.parquet",
            "rules": [
                {
                    "rule_id": "completeness.not_null",
                    "params": {"column": "id"},
                }
            ],
        },
    )

    step_exec = executor.execute(step, ctx, deps)
    assert step_exec.status == ExecutionStatus.SUCCEEDED
    assert step_exec.output_references["quality_score"] == 100.0
    assert ctx.has_step_output("check_customers") is False

    # Failed quality report raises StepExecutionError
    failed_stub = _StubQualityService(
        response={"overall_status": "FAILED", "execution_id": "q_fail"}
    )
    with pytest.raises(StepExecutionError, match="failed with status 'FAILED'"):
        executor.execute(step, ctx, OrchestrationDependencies(quality_service=failed_stub))


def test_executors_reject_missing_dependencies_and_unsupported_actions() -> None:
    ctx = ExecutionContext(workflow_id="wf_err", execution_id="exec_err")
    empty_deps = OrchestrationDependencies()

    for executor, target in (
        (IngestionExecutor(), "atlas-ingestion"),
        (ProcessingExecutor(), "atlas-processing"),
        (QualityExecutor(), "atlas-quality"),
    ):
        bad_action_step = WorkflowStep(
            step_id="s1",
            module_target=target,
            action="invalid_action",
        )
        with pytest.raises(StepExecutionError, match="Unsupported"):
            executor.execute(bad_action_step, ctx, empty_deps)

        missing_dep_step = WorkflowStep(
            step_id="s1",
            module_target=target,
            action="run",
        )
        with pytest.raises(StepExecutionError, match="injected in OrchestrationDependencies"):
            executor.execute(missing_dep_step, ctx, empty_deps)
