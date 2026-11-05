"""Integration tests for atlas-orchestration Phase 1 end-to-end foundation workflows."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from atlas_core import ExecutionStatus
from atlas_orchestration import (
    BaseStepExecutor,
    CyclicDependencyError,
    ExecutionContext,
    InMemoryExecutionTracker,
    OrchestrationDependencies,
    OrchestrationService,
    StepExecution,
    WorkflowDefinition,
    WorkflowExecution,
    WorkflowResult,
    WorkflowStep,
    WorkflowVersionConflictError,
)


class _SimulatedContractExecutor(BaseStepExecutor):
    """Contract-driven step executor used to verify integration across Phase 1 components."""

    def __init__(self, dependencies: OrchestrationDependencies) -> None:
        self._dependencies = dependencies

    def execute(self, step: WorkflowStep, context: ExecutionContext) -> StepExecution:
        t0 = datetime.now(UTC)
        resolved = self._dependencies.resolve_service(step.module_target)
        output = {
            "module_target": step.module_target,
            "action": step.action,
            "service_wired": resolved is not None,
            "region": context.get_variable("region", default="global"),
        }
        context.set_step_output(step.step_id, output)
        t1 = t0 + timedelta(milliseconds=25)
        return StepExecution(
            execution_id=context.execution_id,
            step_id=step.step_id,
            module_target=step.module_target,
            status=ExecutionStatus.SUCCEEDED,
            start_time=t0,
            end_time=t1,
            output_references=output,
        )


def test_end_to_end_workflow_registration_context_and_tracking_foundation() -> None:
    deps = OrchestrationDependencies(
        ingestion_service={"name": "ingestion"},
        processing_service={"name": "processing"},
        quality_service={"name": "quality"},
    )
    tracker = InMemoryExecutionTracker()
    service = OrchestrationService(tracker=tracker, dependencies=deps)

    # 1. Define & register version 1 of a multi-step DAG workflow
    wf_v1 = WorkflowDefinition(
        workflow_id="climate_pipeline",
        name="Climate Pipeline",
        version=1,
        steps=[
            WorkflowStep(
                step_id="ingest_raw",
                module_target="atlas-ingestion",
                action="ingest",
                parameters={"dataset": "noaa_stations"},
            ),
            WorkflowStep(
                step_id="normalize",
                module_target="atlas-processing",
                action="transform",
                parameters={"unit": "celsius"},
                depends_on=["ingest_raw"],
            ),
            WorkflowStep(
                step_id="assess_quality",
                module_target="atlas-quality",
                action="evaluate",
                parameters={"threshold": 0.99},
                depends_on=["normalize"],
            ),
        ],
        metadata={"domain": "climate"},
    )
    service.register_workflow(wf_v1)

    with pytest.raises(WorkflowVersionConflictError):
        service.register_workflow(wf_v1)

    validation = service.validate_workflow(wf_v1)
    assert validation.topological_order == ("ingest_raw", "normalize", "assess_quality")

    # 2. Initialize isolated ExecutionContext and track workflow + step lifecycle
    ctx = ExecutionContext(
        workflow_id=wf_v1.workflow_id,
        workflow_version=wf_v1.version,
        workflow_variables={"region": "eu-central"},
        correlation_id="corr-int-001",
    )

    start_ts = datetime.now(UTC)
    wf_exec = tracker.create_workflow_execution(
        WorkflowExecution(
            execution_id=ctx.execution_id,
            workflow_id=wf_v1.workflow_id,
            workflow_version=wf_v1.version,
            status=ExecutionStatus.RUNNING,
            start_time=start_ts,
            context_overrides={"correlation_id": ctx.correlation_id},
        )
    )

    executor = _SimulatedContractExecutor(service.dependencies)
    for step_id in validation.topological_order:
        step_def = wf_v1.get_step(step_id)
        assert step_def is not None
        step_exec = executor.execute(step_def, ctx)
        tracker.create_step_execution(step_exec)

    tracker.complete(wf_exec.execution_id)
    completed_wf = tracker.get_workflow_execution(wf_exec.execution_id)
    tracked_steps = tracker.list_step_executions(wf_exec.execution_id)
    assert completed_wf.status == ExecutionStatus.SUCCEEDED
    assert len(tracked_steps) == 3

    # 3. Verify context isolation and build final immutable WorkflowResult
    serialized_ctx = ExecutionContext.from_json(ctx.to_json())
    assert serialized_ctx.get_step_output_value("assess_quality", "service_wired") is True
    assert serialized_ctx.get_step_output_value("ingest_raw", "region") == "eu-central"

    result = WorkflowResult(
        execution_id=completed_wf.execution_id,
        workflow_id=completed_wf.workflow_id,
        status=completed_wf.status,
        step_results=tracked_steps,
        duration=0.075,
    )
    assert result.success is True
    assert len(result.step_results) == 3


def test_integration_rejects_cyclic_workflow_before_registration() -> None:
    service = OrchestrationService()
    cyclic_wf = WorkflowDefinition(
        workflow_id="invalid_dag",
        name="Invalid Cyclic DAG",
        version=1,
        steps=[
            WorkflowStep(
                step_id="ingest",
                module_target="atlas-ingestion",
                action="run",
                depends_on=["quality"],
            ),
            WorkflowStep(
                step_id="process",
                module_target="atlas-processing",
                action="run",
                depends_on=["ingest"],
            ),
            WorkflowStep(
                step_id="quality",
                module_target="atlas-quality",
                action="run",
                depends_on=["process"],
            ),
        ],
    )
    with pytest.raises(CyclicDependencyError) as exc_info:
        service.register_workflow(cyclic_wf)

    assert set(exc_info.value.cycle) == {"ingest", "process", "quality"}
    assert service.list_workflows("invalid_dag") == []
