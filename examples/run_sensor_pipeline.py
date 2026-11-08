
from pathlib import Path

from atlas_connectors import ConnectorRegistry, LocalFileConnector
from atlas_core import ExecutionStatus
from atlas_ingestion import IngestionService
from atlas_orchestration import (
    ExecutionContext,
    OrchestrationDependencies,
    OrchestrationService,
    WorkflowDefinition,
    WorkflowStep,
)
from atlas_processing import ProcessingService
from atlas_quality import QualityService
from atlas_storage import FileSystemBackend, StorageManager


# 1. Define workspace paths
WORKSPACE = Path(r"D:\WORK-SPACE\ATLAS-WORKSPACE")

SOURCE = WORKSPACE / "drop" / "corporations-active-cbca-en.csv"

BRONZE = WORKSPACE / "data" / "bronze" / "corporations-active-cbca-en.parquet"
SILVER = WORKSPACE / "data" / "silver" / "corporations-active-cbca-en.parquet"

print("SOURCE:", SOURCE)
print("BRONZE:", BRONZE)
print("SILVER:", SILVER)

# 2. Verify input
if not SOURCE.exists():
    raise FileNotFoundError(f"Input CSV not found: {SOURCE}")

BRONZE.parent.mkdir(parents=True, exist_ok=True)
SILVER.parent.mkdir(parents=True, exist_ok=True)

# 3. Initialize storage
storage = StorageManager(
    backend=FileSystemBackend()
)

# 4. Initialize connector
connectors = ConnectorRegistry()
connectors.register("local_file", LocalFileConnector)

# 5. Initialize actual platform services
ingestion = IngestionService(
    connector_registry=connectors,
    storage_manager=storage,
)

processing = ProcessingService(
    storage_manager=storage,
)

quality = QualityService(
    storage_manager=storage,
)

# 6. Wire services into orchestration
dependencies = OrchestrationDependencies(
    ingestion_service=ingestion,
    processing_service=processing,
    quality_service=quality,
)

orchestrator = OrchestrationService(
    dependencies=dependencies
)

# 7. Define workflow
workflow = WorkflowDefinition(
    workflow_id="sensor_pipeline",
    name="Sensor CSV Pipeline",
    version=1,
    steps=[
        WorkflowStep(
            step_id="ingest",
            module_target="atlas-ingestion",
            action="ingest",
            parameters={
                "dataset_id": "city_corporations-active-cbca-en",
                "source_format": "csv",
                "source": {
                    "source_id": "sensor_csv",
                    "source_name": "Sensor CSV",
                    "source_type": "local_file",
                    "config": {"path": str(SOURCE)},
                },
                "destination": {
                    "layer": "bronze",
                    "path": str(BRONZE),
                    "format": "parquet",
                },
            },
        ),
        WorkflowStep(
            step_id="process",
            module_target="atlas-processing",
            action="transform",
            depends_on=["ingest"],
            parameters={
                "dataset_id": "${steps.ingest.dataset_id}",
                "source": "${steps.ingest.destination}",
                "destination": {
                    "layer": "silver",
                    "path": str(SILVER),
                    "format": "parquet",
                },
                "transformations": [
                    {
                        "transformation_id": "filter",
                        "name": "remove_missing_temperature",
                        "params": {
                            "column": "temperature",
                            "operator": "not_null",
                        },
                    }
                ],
            },
        ),
        WorkflowStep(
            step_id="quality",
            module_target="atlas-quality",
            action="assess",
            depends_on=["process"],
            parameters={
                "dataset_id": "${steps.process.dataset_id}",
                "storage_location": "${steps.process.destination}",
                "rules": [
                    {
                        "rule_id": "completeness.not_null",
                        "name": "sensor_id_required",
                        "params": {"column": "sensor_id"},
                    }
                ],
            },
        ),
    ],
)

# 8. Register and execute
orchestrator.register_workflow(workflow)

context = ExecutionContext(
    workflow_id=workflow.workflow_id,
    workflow_version=workflow.version,
)

result = orchestrator.execute_workflow(
    workflow.workflow_id,
    context=context,
)

# 9. Display results
print("\n========== ATLAS EXECUTION ==========")

print(f"Workflow: {result.workflow_id}")
print(f"Execution: {result.execution_id}")
print(f"Status: {result.status}")

for step in result.step_results:
    print(f"{step.step_id}: {step.status}")

print("====================================")

if result.status != ExecutionStatus.SUCCEEDED:
    raise RuntimeError("Atlas pipeline execution failed")

print(f"Bronze: {BRONZE}")
print(f"Silver: {SILVER}")
