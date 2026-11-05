"""Unit tests for WorkflowEngine covering all Phase 2 acceptance criteria (Tests 1-8)."""

from __future__ import annotations

import pytest
from atlas_core import ExecutionStatus
from atlas_orchestration import (
    ExecutionContext,
    InMemoryExecutionTracker,
    MockStepExecutor,
    StepExecutorRegistry,
    UnknownModuleTargetError,
    UnresolvedContextReferenceError,
    WorkflowDefinition,
    WorkflowEngine,
    WorkflowStep,
    WorkflowValidator,
)


def _create_engine(
    executor: MockStepExecutor | None = None,
    targets: tuple[str, ...] = ("mock-module",),
) -> tuple[WorkflowEngine, MockStepExecutor, InMemoryExecutionTracker]:
    mock_exec = executor or MockStepExecutor()
    registry = StepExecutorRegistry()
    for target in targets:
        registry.register(target, mock_exec)
    tracker = InMemoryExecutionTracker()
    engine = WorkflowEngine(executor_registry=registry, tracker=tracker)
    return engine, mock_exec, tracker


def test_1_topological_execution_follows_validator_order() -> None:
    """Test 1: Definition order differs from dependency order; engine follows topological sort."""
    engine, mock_exec, _ = _create_engine()

    # Defined in reverse order: step_3, step_1, step_2
    # Dependency chain: step_1 -> step_2 -> step_3
    workflow = WorkflowDefinition(
        workflow_id="wf_topo",
        name="Topological Order Test",
        steps=[
            WorkflowStep(
                step_id="step_3",
                module_target="mock-module",
                action="publish",
                depends_on=["step_2"],
            ),
            WorkflowStep(
                step_id="step_1",
                module_target="mock-module",
                action="ingest",
            ),
            WorkflowStep(
                step_id="step_2",
                module_target="mock-module",
                action="transform",
                depends_on=["step_1"],
            ),
        ],
    )

    expected_order = WorkflowValidator().get_topological_order(workflow)
    assert expected_order == ("step_1", "step_2", "step_3")

    result = engine.execute(workflow)
    assert result.status == ExecutionStatus.SUCCEEDED
    assert tuple(mock_exec.executed_step_ids) == expected_order
    assert tuple(s.step_id for s in result.step_results) == expected_order


def test_2_fail_fast_execution_cancels_remaining_steps() -> None:
    """Test 2: Step 2 fails -> Step 1 SUCCEEDED, Step 2 FAILED, Step 3 CANCELLED, WF FAILED."""
    mock_exec = MockStepExecutor(failing_steps={"step_2": "Simulated step 2 failure"})
    engine, _, tracker = _create_engine(mock_exec)

    workflow = WorkflowDefinition(
        workflow_id="wf_fail_fast",
        name="Fail Fast Workflow",
        steps=[
            WorkflowStep(step_id="step_1", module_target="mock-module", action="run"),
            WorkflowStep(
                step_id="step_2",
                module_target="mock-module",
                action="run",
                depends_on=["step_1"],
            ),
            WorkflowStep(
                step_id="step_3",
                module_target="mock-module",
                action="run",
                depends_on=["step_2"],
            ),
        ],
    )

    result = engine.execute(workflow)

    assert result.status == ExecutionStatus.FAILED
    assert result.success is False
    assert mock_exec.executed_step_ids == ["step_1", "step_2"]
    assert "step_3" not in mock_exec.executed_step_ids

    steps_by_id = {s.step_id: s for s in result.step_results}
    assert steps_by_id["step_1"].status == ExecutionStatus.SUCCEEDED
    assert steps_by_id["step_2"].status == ExecutionStatus.FAILED
    assert "Simulated step 2 failure" in (steps_by_id["step_2"].error_message or "")
    assert steps_by_id["step_3"].status == ExecutionStatus.CANCELLED

    wf_exec = tracker.get_workflow_execution(result.execution_id)
    assert wf_exec.status == ExecutionStatus.FAILED


def test_3_context_propagation_between_steps_and_type_preservation() -> None:
    """Test 3: Step 1 output is dynamically resolved into Step 2 parameters."""
    mock_exec = MockStepExecutor(
        step_outputs={
            "step_1": {
                "artifact_path": "data/customers.parquet",
                "record_count": 1500,
                "source_name": "customer_api",
            }
        }
    )
    engine, _, _ = _create_engine(mock_exec)

    workflow = WorkflowDefinition(
        workflow_id="wf_ctx_prop",
        name="Context Propagation",
        steps=[
            WorkflowStep(
                step_id="step_1",
                module_target="mock-module",
                action="ingest",
            ),
            WorkflowStep(
                step_id="step_2",
                module_target="mock-module",
                action="transform",
                depends_on=["step_1"],
                parameters={
                    "input_path": "${steps.step_1.artifact_path}",
                    "expected_rows": "${steps.step_1.record_count}",
                    "options": {
                        "format": "parquet",
                        "source": "${steps.step_1.source_name}",
                    },
                },
            ),
        ],
    )

    result = engine.execute(workflow)
    assert result.status == ExecutionStatus.SUCCEEDED

    executed_step_2 = mock_exec.executed_steps[1]
    assert executed_step_2.step_id == "step_2"
    assert executed_step_2.parameters == {
        "input_path": "data/customers.parquet",
        "expected_rows": 1500,
        "options": {
            "format": "parquet",
            "source": "customer_api",
        },
    }
    assert isinstance(executed_step_2.parameters["expected_rows"], int)

    # Original workflow definition remains unmutated
    orig_step_2 = workflow.get_step("step_2")
    assert orig_step_2 is not None
    assert orig_step_2.parameters["input_path"] == "${steps.step_1.artifact_path}"


def test_4_resolution_failure_stops_and_cancels_downstream_steps() -> None:
    """Test 4: Referencing nonexistent step/output raises UnresolvedContextReferenceError."""
    engine, mock_exec, _ = _create_engine()

    workflow = WorkflowDefinition(
        workflow_id="wf_bad_ref",
        name="Unresolved Reference Workflow",
        steps=[
            WorkflowStep(step_id="step_1", module_target="mock-module", action="ingest"),
            WorkflowStep(
                step_id="step_2",
                module_target="mock-module",
                action="transform",
                depends_on=["step_1"],
                parameters={"input_path": "${steps.step_1.nonexistent_output}"},
            ),
            WorkflowStep(
                step_id="step_3",
                module_target="mock-module",
                action="quality",
                depends_on=["step_2"],
            ),
        ],
    )

    # Verify raise_on_error=True surfaces UnresolvedContextReferenceError directly
    with pytest.raises(UnresolvedContextReferenceError):
        engine.execute(workflow, execution_id="exec_raise", raise_on_error=True)

    # Verify default execution records failure and cancels step_3 without running step_2 or step_3
    mock_exec.executed_steps.clear()
    result = engine.execute(workflow, execution_id="exec_return")
    assert result.status == ExecutionStatus.FAILED
    assert mock_exec.executed_step_ids == ["step_1"]

    steps_by_id = {s.step_id: s for s in result.step_results}
    assert steps_by_id["step_1"].status == ExecutionStatus.SUCCEEDED
    assert steps_by_id["step_2"].status == ExecutionStatus.FAILED
    assert steps_by_id["step_2"].error_code == "UnresolvedContextReferenceError"
    assert steps_by_id["step_3"].status == ExecutionStatus.CANCELLED


def test_5_unknown_module_target_rejected_with_clear_error() -> None:
    """Test 5: Submitting a workflow with an unknown module_target fails clearly."""
    engine, mock_exec, _ = _create_engine(targets=("atlas-ingestion", "atlas-processing"))

    workflow = WorkflowDefinition(
        workflow_id="wf_unknown_target",
        name="Unknown Target Workflow",
        steps=[
            WorkflowStep(step_id="step_1", module_target="atlas-ingestion", action="ingest"),
            WorkflowStep(
                step_id="step_2",
                module_target="atlas-unregistered-target",
                action="run",
                depends_on=["step_1"],
            ),
            WorkflowStep(
                step_id="step_3",
                module_target="atlas-processing",
                action="transform",
                depends_on=["step_2"],
            ),
        ],
    )

    with pytest.raises(UnknownModuleTargetError):
        engine.execute(workflow, execution_id="exec_unk_1", raise_on_error=True)

    result = engine.execute(workflow, execution_id="exec_unk_2")
    assert result.status == ExecutionStatus.FAILED
    steps_by_id = {s.step_id: s for s in result.step_results}
    assert steps_by_id["step_1"].status == ExecutionStatus.SUCCEEDED
    assert steps_by_id["step_2"].status == ExecutionStatus.FAILED
    assert steps_by_id["step_2"].error_code == "UnknownModuleTargetError"
    assert steps_by_id["step_3"].status == ExecutionStatus.CANCELLED


def test_6_happy_path_full_workflow_execution() -> None:
    """Test 6: Every step and workflow SUCCEEDED, outputs in context, WorkflowResult valid."""
    mock_exec = MockStepExecutor(
        step_outputs={
            "s1": {"artifact_path": "bronze/orders.parquet", "rows": 250},
            "s2": {"artifact_path": "silver/orders.parquet", "rows": 245},
            "s3": {"quality_score": 100.0, "overall_status": "PASSED"},
        }
    )
    engine, _, tracker = _create_engine(mock_exec)

    workflow = WorkflowDefinition(
        workflow_id="wf_happy",
        name="Happy Path Workflow",
        steps=[
            WorkflowStep(step_id="s1", module_target="mock-module", action="ingest"),
            WorkflowStep(
                step_id="s2",
                module_target="mock-module",
                action="process",
                depends_on=["s1"],
                parameters={"src": "${steps.s1.artifact_path}"},
            ),
            WorkflowStep(
                step_id="s3",
                module_target="mock-module",
                action="assess",
                depends_on=["s2"],
                parameters={"target": "${steps.s2.artifact_path}"},
            ),
        ],
    )

    context = ExecutionContext(workflow_id=workflow.workflow_id, workflow_version=workflow.version)
    result = engine.execute(workflow, context=context)

    assert result.success is True
    assert result.status == ExecutionStatus.SUCCEEDED
    assert result.duration >= 0.0
    assert len(result.step_results) == 3
    assert all(s.status == ExecutionStatus.SUCCEEDED for s in result.step_results)

    assert context.get_step_output("s1") == {
        "artifact_path": "bronze/orders.parquet",
        "rows": 250,
    }
    assert context.get_step_output("s2") == {
        "artifact_path": "silver/orders.parquet",
        "rows": 245,
    }
    assert context.get_step_output("s3") == {
        "quality_score": 100.0,
        "overall_status": "PASSED",
    }

    wf_exec = tracker.get_workflow_execution(result.execution_id)
    assert wf_exec.status == ExecutionStatus.SUCCEEDED
    assert wf_exec.start_time is not None
    assert wf_exec.end_time is not None


def test_7_execution_isolation_between_separate_runs() -> None:
    """Test 7: Outputs from one execution cannot leak into another execution."""
    call_counter = 0

    def dynamic_handler(step: WorkflowStep, ctx: ExecutionContext) -> dict[str, object]:
        nonlocal call_counter
        call_counter += 1
        return {
            "run_number": call_counter,
            "tenant": ctx.get_variable("tenant"),
            "shared_step_data": [call_counter],
        }

    mock_exec = MockStepExecutor(handler=dynamic_handler)
    engine, _, _ = _create_engine(mock_exec)

    workflow = WorkflowDefinition(
        workflow_id="wf_isolated",
        name="Isolation Workflow",
        steps=[
            WorkflowStep(step_id="step_1", module_target="mock-module", action="run"),
            WorkflowStep(
                step_id="step_2",
                module_target="mock-module",
                action="run",
                depends_on=["step_1"],
                parameters={
                    "prev_tenant": "${steps.step_1.tenant}",
                    "prev_run": "${steps.step_1.run_number}",
                },
            ),
        ],
    )

    ctx_a = ExecutionContext(workflow_id="wf_isolated")
    ctx_b = ExecutionContext(workflow_id="wf_isolated")

    res_a = engine.execute(workflow, parameters={"tenant": "tenant_a"}, context=ctx_a)
    res_b = engine.execute(workflow, parameters={"tenant": "tenant_b"}, context=ctx_b)

    assert res_a.execution_id != res_b.execution_id
    assert ctx_a.get_step_output_value("step_1", "tenant") == "tenant_a"
    assert ctx_b.get_step_output_value("step_1", "tenant") == "tenant_b"
    assert ctx_a.get_step_output_value("step_1", "run_number") == 1
    assert ctx_b.get_step_output_value("step_1", "run_number") == 3

    # Step 2 in run B only saw run B's step_1 outputs
    step_2_run_b = mock_exec.executed_steps[3]
    assert step_2_run_b.parameters == {"prev_tenant": "tenant_b", "prev_run": 3}


def test_8_executor_unexpected_exception_handling() -> None:
    """Test 8: Unexpected RuntimeError in executor is captured; remaining steps cancelled."""
    mock_exec = MockStepExecutor(
        failing_steps={"step_2": RuntimeError("Unexpected low-level crash")}
    )
    engine, _, tracker = _create_engine(mock_exec)

    workflow = WorkflowDefinition(
        workflow_id="wf_crash",
        name="Crash Handling Workflow",
        steps=[
            WorkflowStep(step_id="step_1", module_target="mock-module", action="run"),
            WorkflowStep(
                step_id="step_2",
                module_target="mock-module",
                action="run",
                depends_on=["step_1"],
            ),
            WorkflowStep(
                step_id="step_3",
                module_target="mock-module",
                action="run",
                depends_on=["step_2"],
            ),
        ],
    )

    result = engine.execute(workflow)

    assert result.status == ExecutionStatus.FAILED
    assert result.message == "Unexpected low-level crash"
    assert mock_exec.executed_step_ids == ["step_1", "step_2"]

    steps = {s.step_id: s for s in tracker.list_step_executions(result.execution_id)}
    assert steps["step_1"].status == ExecutionStatus.SUCCEEDED
    assert steps["step_2"].status == ExecutionStatus.FAILED
    assert steps["step_2"].error_code == "RuntimeError"
    assert steps["step_2"].error_message == "Unexpected low-level crash"
    assert steps["step_3"].status == ExecutionStatus.CANCELLED
