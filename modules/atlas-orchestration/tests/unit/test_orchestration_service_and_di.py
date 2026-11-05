"""Unit tests for OrchestrationService and OrchestrationDependencies."""

from __future__ import annotations

import pytest
from atlas_core import ExecutionStatus
from atlas_orchestration import (
    InMemoryExecutionTracker,
    OrchestrationDependencies,
    OrchestrationService,
    StepExecution,
    WorkflowDefinition,
    WorkflowExecution,
    WorkflowNotFoundError,
    WorkflowRegistry,
    WorkflowStep,
    WorkflowValidator,
)


def test_orchestration_dependencies_resolution() -> None:
    ingestion_stub = object()
    processing_stub = object()
    quality_stub = object()
    storage_stub = object()
    metadata_stub = object()
    custom_stub = object()

    deps = OrchestrationDependencies(
        ingestion_service=ingestion_stub,
        processing_service=processing_stub,
        quality_service=quality_stub,
        storage_provider=storage_stub,
        metadata_provider=metadata_stub,
        extra_services={"custom-engine": custom_stub},
    )

    assert deps.resolve_service("atlas-ingestion") is ingestion_stub
    assert deps.resolve_service("ingestion") is ingestion_stub
    assert deps.resolve_service("atlas-processing") is processing_stub
    assert deps.resolve_service("atlas-quality") is quality_stub
    assert deps.resolve_service("atlas-storage") is storage_stub
    assert deps.resolve_service("atlas-metadata") is metadata_stub
    assert deps.resolve_service("custom-engine") is custom_stub
    assert deps.resolve_service("unknown") is None
    assert deps.resolve_service("") is None


def test_orchestration_service_workflow_lifecycle_and_tracking_queries() -> None:
    validator = WorkflowValidator()
    registry = WorkflowRegistry(validator=validator)
    tracker = InMemoryExecutionTracker()
    deps = OrchestrationDependencies(ingestion_service="mock_ingestion")

    service = OrchestrationService(
        registry=registry,
        validator=validator,
        tracker=tracker,
        dependencies=deps,
    )

    assert service.registry is registry
    assert service.validator is validator
    assert service.tracker is tracker
    assert service.dependencies is deps

    wf_v1 = WorkflowDefinition(
        workflow_id="wf_etl",
        name="ETL Pipeline",
        version=1,
        steps=[
            WorkflowStep(step_id="extract", module_target="atlas-ingestion", action="ingest"),
            WorkflowStep(
                step_id="transform",
                module_target="atlas-processing",
                action="clean",
                depends_on=["extract"],
            ),
        ],
    )
    wf_v2 = WorkflowDefinition(
        workflow_id="wf_etl",
        name="ETL Pipeline v2",
        version=2,
        steps=[
            WorkflowStep(step_id="extract", module_target="atlas-ingestion", action="ingest"),
        ],
    )

    val_res = service.validate_workflow(wf_v1)
    assert val_res.is_valid is True
    assert val_res.topological_order == ("extract", "transform")

    service.register_workflow(wf_v1)
    service.register_workflow(wf_v2)

    assert service.get_workflow("wf_etl", version=1) == wf_v1
    assert service.get_latest_workflow("wf_etl") == wf_v2
    assert len(service.list_workflows("wf_etl")) == 2

    # Record executions in tracker and query through service
    wf_exec = tracker.create_workflow_execution(
        WorkflowExecution(
            execution_id="exec_svc_1",
            workflow_id="wf_etl",
            workflow_version=2,
            status=ExecutionStatus.RUNNING,
        )
    )
    step_exec = tracker.create_step_execution(
        StepExecution(
            step_execution_id="step_svc_1",
            execution_id="exec_svc_1",
            step_id="extract",
            module_target="atlas-ingestion",
            status=ExecutionStatus.SUCCEEDED,
        )
    )

    assert service.get_workflow_execution("exec_svc_1").execution_id == wf_exec.execution_id
    assert len(service.list_workflow_executions(workflow_id="wf_etl")) == 1
    assert service.get_step_execution("step_svc_1").step_id == step_exec.step_id
    assert len(service.list_step_executions("exec_svc_1")) == 1

    service.unregister_workflow("wf_etl", version=1)
    with pytest.raises(WorkflowNotFoundError):
        service.get_workflow("wf_etl", version=1)
    assert service.get_latest_workflow("wf_etl") == wf_v2
