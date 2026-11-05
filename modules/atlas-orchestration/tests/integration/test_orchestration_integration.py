"""Integration tests for atlas-orchestration Phase 1 & Phase 2 sequential execution."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from atlas_connectors import ConnectorRegistry, LocalFileConnector
from atlas_core import ExecutionStatus
from atlas_ingestion import IngestionService
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
from atlas_processing import ProcessingService
from atlas_quality import QualityService
from atlas_storage import FileSystemBackend, StorageManager


class _SimulatedContractExecutor(BaseStepExecutor):
    """Contract-driven step executor used to verify integration across Phase 1 components."""

    def __init__(self, dependencies: OrchestrationDependencies) -> None:
        self._dependencies = dependencies

    def execute(
        self,
        step: WorkflowStep,
        context: ExecutionContext,
        dependencies: OrchestrationDependencies | None = None,
    ) -> StepExecution:
        t0 = datetime.now(UTC)
        deps = dependencies or self._dependencies
        resolved = deps.resolve_service(step.module_target)
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


def test_phase2_end_to_end_sequential_execution_across_real_platform_services(
    tmp_path: Path,
) -> None:
    """Execute a real 3-step workflow across Ingestion, Processing, and Quality services."""
    raw_csv = tmp_path / "sensors_raw.csv"
    raw_csv.write_text(
        "sensor_id,temperature,city\ns1,21.5,Toronto\ns2,24.0,Montreal\ns3,,Vancouver\n",
        encoding="utf-8",
    )

    bronze_path = str(tmp_path / "bronze" / "sensors_raw.parquet")
    silver_path = str(tmp_path / "silver" / "sensors_curated.parquet")

    backend = FileSystemBackend()
    storage_manager = StorageManager(backend=backend)
    connector_registry = ConnectorRegistry()
    connector_registry.register("local_file", LocalFileConnector)

    ingestion_service = IngestionService(
        connector_registry=connector_registry,
        storage_manager=storage_manager,
    )
    processing_service = ProcessingService(storage_manager=storage_manager)
    quality_service = QualityService(storage_manager=storage_manager)

    dependencies = OrchestrationDependencies(
        ingestion_service=ingestion_service,
        processing_service=processing_service,
        quality_service=quality_service,
    )
    orchestration = OrchestrationService(dependencies=dependencies)

    workflow = WorkflowDefinition(
        workflow_id="sensor_curation_workflow",
        name="Sensor Ingestion, Processing, and Quality Workflow",
        version=1,
        steps=[
            WorkflowStep(
                step_id="quality_step",
                module_target="atlas-quality",
                action="assess",
                depends_on=["process_step"],
                parameters={
                    "dataset_id": "${steps.process_step.dataset_id}",
                    "storage_location": "${steps.process_step.destination}",
                    "rules": [
                        {
                            "rule_id": "completeness.not_null",
                            "name": "sensor_id_not_null",
                            "params": {"column": "sensor_id"},
                        },
                        {
                            "rule_id": "completeness.not_null",
                            "name": "temp_not_null",
                            "params": {"column": "temperature"},
                        },
                    ],
                },
            ),
            WorkflowStep(
                step_id="ingest_step",
                module_target="atlas-ingestion",
                action="ingest",
                parameters={
                    "dataset_id": "city_sensors",
                    "source_format": "csv",
                    "source": {
                        "source_id": "local_sensor_csv",
                        "source_name": "Local Sensor CSV",
                        "source_type": "local_file",
                        "config": {"path": str(raw_csv)},
                    },
                    "destination": {
                        "layer": "bronze",
                        "path": bronze_path,
                        "format": "parquet",
                    },
                },
            ),
            WorkflowStep(
                step_id="process_step",
                module_target="atlas-processing",
                action="transform",
                depends_on=["ingest_step"],
                parameters={
                    "dataset_id": "${steps.ingest_step.dataset_id}",
                    "source": "${steps.ingest_step.destination}",
                    "destination": {
                        "layer": "silver",
                        "path": silver_path,
                        "format": "parquet",
                    },
                    "transformations": [
                        {
                            "transformation_id": "filter",
                            "name": "drop_missing_temp",
                            "params": {
                                "column": "temperature",
                                "operator": "not_null",
                            },
                        }
                    ],
                },
            ),
        ],
    )

    orchestration.register_workflow(workflow)
    context = ExecutionContext(workflow_id=workflow.workflow_id, workflow_version=1)
    result = orchestration.execute_workflow("sensor_curation_workflow", context=context)

    assert result.success is True
    assert result.status == ExecutionStatus.SUCCEEDED
    assert [s.step_id for s in result.step_results] == [
        "ingest_step",
        "process_step",
        "quality_step",
    ]
    assert all(s.status == ExecutionStatus.SUCCEEDED for s in result.step_results)

    # Verify propagated context outputs across all three modules
    assert context.get_step_output_value("ingest_step", "row_count") == 3
    assert context.get_step_output_value("process_step", "input_row_count") == 3
    assert context.get_step_output_value("process_step", "output_row_count") == 2
    assert context.get_step_output_value("quality_step", "overall_status") == "PASSED"
    assert context.get_step_output_value("quality_step", "quality_score") == 100.0
