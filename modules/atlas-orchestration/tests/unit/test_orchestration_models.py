"""Unit tests for atlas-orchestration domain models and exception hierarchy."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from atlas_core import (
    Execution,
    ExecutionStatus,
    Result,
)
from atlas_core import (
    OrchestrationError as CoreOrchestrationError,
)
from atlas_orchestration import (
    CyclicDependencyError,
    DuplicateWorkflowError,
    InvalidStepDefinitionError,
    InvalidWorkflowDefinitionError,
    InvalidWorkflowDependencyError,
    InvalidWorkflowStateError,
    OrchestrationError,
    StepExecution,
    WorkflowDefinition,
    WorkflowExecution,
    WorkflowResult,
    WorkflowStep,
    WorkflowVersionConflictError,
)
from pydantic import ValidationError


def test_exception_hierarchy_inherits_atlas_core() -> None:
    err = WorkflowVersionConflictError(
        "conflict",
        workflow_id="wf_1",
        workflow_version=2,
        execution_id="ex_1",
        step_id="st_1",
        step_execution_id="st_ex_1",
    )
    assert isinstance(err, DuplicateWorkflowError)
    assert isinstance(err, OrchestrationError)
    assert isinstance(err, CoreOrchestrationError)
    assert err.workflow_id == "wf_1"
    assert err.workflow_version == 2
    assert err.execution_id == "ex_1"
    assert err.step_id == "st_1"
    assert err.step_execution_id == "st_ex_1"

    cycle_err = CyclicDependencyError(
        "cycle",
        cycle=("a", "b", "a"),
        workflow_id="wf_1",
        workflow_version=1,
    )
    assert isinstance(cycle_err, InvalidWorkflowDependencyError)
    assert isinstance(cycle_err, InvalidWorkflowDefinitionError)
    assert cycle_err.cycle == ("a", "b", "a")


def test_workflow_step_valid_and_immutable() -> None:
    params = {"batch_size": 100, "tags": ["raw", "daily"]}
    step = WorkflowStep(
        step_id=" ingest_raw ",
        module_target=" atlas-ingestion ",
        action=" ingest ",
        parameters=params,
        depends_on=[" prep_step "],
    )
    assert step.step_id == "ingest_raw"
    assert step.module_target == "atlas-ingestion"
    assert step.action == "ingest"
    assert step.depends_on == ["prep_step"]

    # Mutating caller dict does not affect stored parameters
    params["batch_size"] = 999
    assert step.parameters["batch_size"] == 100

    with pytest.raises(ValidationError):
        step.step_id = "other"


@pytest.mark.parametrize(
    ("step_id", "target", "action"),
    [
        ("", "atlas-ingestion", "ingest"),
        ("   ", "atlas-ingestion", "ingest"),
        ("s1", "", "ingest"),
        ("s1", "atlas-ingestion", "  "),
    ],
)
def test_workflow_step_rejects_empty_required_fields(
    step_id: str,
    target: str,
    action: str,
) -> None:
    with pytest.raises(InvalidStepDefinitionError):
        WorkflowStep(step_id=step_id, module_target=target, action=action)


def test_workflow_step_rejects_self_and_duplicate_dependencies() -> None:
    with pytest.raises(InvalidWorkflowDependencyError, match="cannot depend on itself"):
        WorkflowStep(
            step_id="step_a",
            module_target="atlas-processing",
            action="run",
            depends_on=["step_a"],
        )

    with pytest.raises(InvalidWorkflowDependencyError, match="duplicate dependency"):
        WorkflowStep(
            step_id="step_b",
            module_target="atlas-processing",
            action="run",
            depends_on=["step_a", "step_a"],
        )

    with pytest.raises(InvalidStepDefinitionError, match="empty dependency"):
        WorkflowStep(
            step_id="step_b",
            module_target="atlas-processing",
            action="run",
            depends_on=["   "],
        )


def test_workflow_step_rejects_non_json_parameters() -> None:
    with pytest.raises(InvalidStepDefinitionError, match="not JSON-compatible"):
        WorkflowStep(
            step_id="step_a",
            module_target="atlas-processing",
            action="run",
            parameters={"bad": object()},
        )

    with pytest.raises(InvalidStepDefinitionError, match="Non-finite float"):
        WorkflowStep(
            step_id="step_a",
            module_target="atlas-processing",
            action="run",
            parameters={"nan_val": float("nan")},
        )


def test_workflow_definition_validation_and_get_step() -> None:
    s1 = WorkflowStep(step_id="s1", module_target="atlas-ingestion", action="extract")
    s2 = WorkflowStep(
        step_id="s2",
        module_target="atlas-processing",
        action="transform",
        depends_on=["s1"],
    )
    wf = WorkflowDefinition(
        workflow_id=" wf_daily ",
        name=" Daily Pipeline ",
        description="Daily ETL",
        version=2,
        steps=[s1, s2],
        metadata={"owner": "data-eng"},
    )
    assert wf.workflow_id == "wf_daily"
    assert wf.name == "Daily Pipeline"
    assert wf.version == 2
    assert len(wf.steps) == 2
    assert wf.get_step("s1") == s1
    assert wf.get_step("unknown") is None


def test_workflow_definition_rejects_invalid_inputs() -> None:
    step = WorkflowStep(step_id="s1", module_target="atlas-ingestion", action="extract")

    with pytest.raises(InvalidWorkflowDefinitionError):
        WorkflowDefinition(workflow_id="", name="Valid", steps=[step])

    with pytest.raises(InvalidWorkflowDefinitionError):
        WorkflowDefinition(workflow_id="wf_1", name="   ", steps=[step])

    with pytest.raises(InvalidWorkflowDefinitionError, match="version must be"):
        WorkflowDefinition(workflow_id="wf_1", name="Valid", version=0, steps=[step])

    with pytest.raises(InvalidWorkflowDefinitionError, match="version must be"):
        WorkflowDefinition(workflow_id="wf_1", name="Valid", version=True, steps=[step])

    with pytest.raises(InvalidWorkflowDefinitionError, match="at least one WorkflowStep"):
        WorkflowDefinition(workflow_id="wf_1", name="Valid", steps=[])

    with pytest.raises(InvalidStepDefinitionError, match="Duplicate step_id"):
        WorkflowDefinition(workflow_id="wf_1", name="Valid", steps=[step, step])

    with pytest.raises(InvalidWorkflowDefinitionError, match="not JSON-compatible"):
        WorkflowDefinition(
            workflow_id="wf_1",
            name="Valid",
            steps=[step],
            metadata={"invalid": {1, 2, 3}},
        )


def test_step_execution_lifecycle_and_validation() -> None:
    t0 = datetime.now(UTC)
    t1 = t0 + timedelta(seconds=5)

    step_exec = StepExecution(
        execution_id="exec_100",
        step_id="step_1",
        module_target="atlas-ingestion",
        status=ExecutionStatus.SUCCEEDED,
        attempt_number=1,
        start_time=t0,
        end_time=t1,
        module_execution_ref="ing_exec_99",
        output_references={"rows": 42},
    )
    assert step_exec.execution_id == "exec_100"
    assert step_exec.step_id == "step_1"
    assert step_exec.module_target == "atlas-ingestion"
    assert step_exec.status == ExecutionStatus.SUCCEEDED
    assert step_exec.output_references == {"rows": 42}
    assert step_exec.module_execution_ref == "ing_exec_99"
    assert step_exec.step_execution_id

    # End time before start time
    with pytest.raises(InvalidWorkflowStateError, match="cannot precede"):
        StepExecution(
            execution_id="exec_100",
            step_id="step_1",
            module_target="atlas-ingestion",
            start_time=t1,
            end_time=t0,
        )

    # Naive timestamp rejected
    with pytest.raises(InvalidWorkflowStateError, match="timezone-aware"):
        StepExecution(
            execution_id="exec_100",
            step_id="step_1",
            module_target="atlas-ingestion",
            start_time=datetime(2026, 1, 1, 12, 0, 0),
        )


def test_workflow_execution_extends_atlas_core_execution() -> None:
    t0 = datetime.now(UTC)
    t1 = t0 + timedelta(seconds=12)

    wf_exec = WorkflowExecution(
        execution_id="exec_200",
        workflow_id="wf_heat",
        workflow_version=3,
        status=ExecutionStatus.SUCCEEDED,
        start_time=t0,
        end_time=t1,
        context_overrides={"trigger": "manual"},
    )
    assert isinstance(wf_exec, Execution)
    assert wf_exec.id == "exec_200"
    assert wf_exec.execution_id == "exec_200"
    assert wf_exec.started_at == t0
    assert wf_exec.completed_at == t1
    assert wf_exec.workflow_version == 3
    assert wf_exec.context_overrides == {"trigger": "manual"}

    with pytest.raises(InvalidWorkflowStateError, match="cannot precede"):
        WorkflowExecution(
            execution_id="exec_200",
            workflow_id="wf_heat",
            start_time=t1,
            end_time=t0,
        )


def test_workflow_result_extends_atlas_core_result() -> None:
    step_exec = StepExecution(
        execution_id="exec_300",
        step_id="s1",
        module_target="atlas-ingestion",
        status=ExecutionStatus.SUCCEEDED,
    )
    res = WorkflowResult(
        execution_id="exec_300",
        workflow_id="wf_heat",
        status=ExecutionStatus.SUCCEEDED,
        step_results=[step_exec],
        duration=2.5,
        message="Completed successfully",
    )
    assert isinstance(res, Result)
    assert res.success is True
    assert res.duration == 2.5
    assert len(res.step_results) == 1

    failed_res = WorkflowResult(
        execution_id="exec_301",
        workflow_id="wf_heat",
        status=ExecutionStatus.FAILED,
        duration=1.0,
        message="Step s1 failed",
    )
    assert failed_res.success is False
    assert failed_res.message == "Step s1 failed"

    # Mismatched step execution_id rejected
    foreign_step = StepExecution(
        execution_id="other_exec",
        step_id="s1",
        module_target="atlas-ingestion",
    )
    with pytest.raises(InvalidWorkflowStateError, match="belongs to execution_id"):
        WorkflowResult(
            execution_id="exec_300",
            workflow_id="wf_heat",
            status=ExecutionStatus.SUCCEEDED,
            step_results=[foreign_step],
        )

    with pytest.raises(InvalidWorkflowStateError, match="duration must be >= 0.0"):
        WorkflowResult(
            execution_id="exec_302",
            workflow_id="wf_heat",
            status=ExecutionStatus.FAILED,
            duration=-0.1,
        )
