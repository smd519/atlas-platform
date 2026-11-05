"""Unit tests for WorkflowValidator covering DAG validation, dependencies, and cycles."""

from __future__ import annotations

import pytest
from atlas_orchestration import (
    CyclicDependencyError,
    InvalidStepDefinitionError,
    InvalidWorkflowDefinitionError,
    InvalidWorkflowDependencyError,
    WorkflowDefinition,
    WorkflowStep,
    WorkflowValidator,
)


def test_validate_linear_and_diamond_dag_topological_order() -> None:
    validator = WorkflowValidator()
    workflow = WorkflowDefinition(
        workflow_id="diamond_wf",
        name="Diamond DAG Workflow",
        version=1,
        steps=[
            WorkflowStep(step_id="ingest", module_target="atlas-ingestion", action="run"),
            WorkflowStep(
                step_id="branch_a",
                module_target="atlas-processing",
                action="clean_a",
                depends_on=["ingest"],
            ),
            WorkflowStep(
                step_id="branch_b",
                module_target="atlas-processing",
                action="clean_b",
                depends_on=["ingest"],
            ),
            WorkflowStep(
                step_id="quality_join",
                module_target="atlas-quality",
                action="check",
                depends_on=["branch_a", "branch_b"],
            ),
        ],
    )
    result = validator.validate(workflow)
    assert result.is_valid is True
    assert result.workflow_id == "diamond_wf"
    assert result.workflow_version == 1
    assert result.topological_order == ("ingest", "branch_a", "branch_b", "quality_join")
    assert validator.get_topological_order(workflow) == result.topological_order


def test_validator_rejects_unknown_step_dependency() -> None:
    validator = WorkflowValidator()
    workflow = WorkflowDefinition(
        workflow_id="broken_dep_wf",
        name="Broken Dependency Workflow",
        steps=[
            WorkflowStep(
                step_id="step_1",
                module_target="atlas-ingestion",
                action="run",
                depends_on=["non_existent_step"],
            )
        ],
    )
    with pytest.raises(
        InvalidWorkflowDependencyError,
        match="unknown step 'non_existent_step'",
    ) as exc_info:
        validator.validate(workflow)
    assert exc_info.value.workflow_id == "broken_dep_wf"
    assert exc_info.value.step_id == "step_1"


def test_validator_detects_direct_two_node_cycle() -> None:
    validator = WorkflowValidator()
    workflow = WorkflowDefinition(
        workflow_id="direct_cycle_wf",
        name="Direct Cycle",
        steps=[
            WorkflowStep(
                step_id="step_a",
                module_target="atlas-ingestion",
                action="run",
                depends_on=["step_b"],
            ),
            WorkflowStep(
                step_id="step_b",
                module_target="atlas-processing",
                action="run",
                depends_on=["step_a"],
            ),
        ],
    )
    with pytest.raises(CyclicDependencyError) as exc_info:
        validator.validate(workflow)

    cycle = exc_info.value.cycle
    assert len(cycle) == 3
    assert cycle[0] == cycle[-1]
    assert set(cycle) == {"step_a", "step_b"}


def test_validator_detects_indirect_multi_step_cycle() -> None:
    validator = WorkflowValidator()
    workflow = WorkflowDefinition(
        workflow_id="indirect_cycle_wf",
        name="Indirect Cycle",
        steps=[
            WorkflowStep(
                step_id="step_a",
                module_target="atlas-ingestion",
                action="run",
                depends_on=["step_c"],
            ),
            WorkflowStep(
                step_id="step_b",
                module_target="atlas-processing",
                action="run",
                depends_on=["step_a"],
            ),
            WorkflowStep(
                step_id="step_c",
                module_target="atlas-quality",
                action="run",
                depends_on=["step_b"],
            ),
        ],
    )
    with pytest.raises(CyclicDependencyError) as exc_info:
        validator.validate(workflow)

    cycle = exc_info.value.cycle
    assert len(cycle) == 4
    assert cycle[0] == cycle[-1]
    assert set(cycle) == {"step_a", "step_b", "step_c"}


def test_validator_detects_cycle_in_complex_multi_chain_graph() -> None:
    validator = WorkflowValidator()
    workflow = WorkflowDefinition(
        workflow_id="complex_cycle_wf",
        name="Complex Graph With Cycle",
        steps=[
            WorkflowStep(step_id="root", module_target="atlas-ingestion", action="ingest"),
            WorkflowStep(
                step_id="prep",
                module_target="atlas-processing",
                action="prep",
                depends_on=["root"],
            ),
            # Disconnected / downstream cyclic subgraph: c1 -> c2 -> c3 -> c1
            WorkflowStep(
                step_id="c1",
                module_target="atlas-processing",
                action="s1",
                depends_on=["prep", "c3"],
            ),
            WorkflowStep(
                step_id="c2",
                module_target="atlas-processing",
                action="s2",
                depends_on=["c1"],
            ),
            WorkflowStep(
                step_id="c3",
                module_target="atlas-quality",
                action="s3",
                depends_on=["c2"],
            ),
            WorkflowStep(
                step_id="leaf",
                module_target="atlas-metadata",
                action="publish",
                depends_on=["c3"],
            ),
        ],
    )
    with pytest.raises(CyclicDependencyError) as exc_info:
        validator.validate(workflow)

    cycle = exc_info.value.cycle
    assert cycle[0] == cycle[-1]
    assert set(cycle) == {"c1", "c2", "c3"}


def test_validator_guards_against_model_construct_bypass() -> None:
    validator = WorkflowValidator()

    with pytest.raises(InvalidWorkflowDefinitionError):
        validator.validate("not-a-workflow")  # type: ignore[arg-type]

    # Bypass Pydantic init using model_construct to ensure validator still checks invariants
    bad_step = WorkflowStep.model_construct(
        step_id="s1",
        module_target="atlas-ingestion",
        action="run",
        parameters={},
        depends_on=["s1"],
    )
    bypassed_wf = WorkflowDefinition.model_construct(
        workflow_id="wf_bypass",
        name="Bypass",
        version=1,
        steps=[bad_step],
        metadata={},
    )
    with pytest.raises(InvalidWorkflowDependencyError, match="cannot depend on itself"):
        validator.validate(bypassed_wf)

    dup_step = WorkflowStep.model_construct(
        step_id="s1",
        module_target="atlas-ingestion",
        action="run",
        parameters={},
        depends_on=[],
    )
    dup_wf = WorkflowDefinition.model_construct(
        workflow_id="wf_dup",
        name="Dup",
        version=1,
        steps=[dup_step, dup_step],
        metadata={},
    )
    with pytest.raises(InvalidStepDefinitionError, match="Duplicate step_id"):
        validator.validate(dup_wf)
