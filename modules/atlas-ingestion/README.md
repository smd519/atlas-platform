# atlas-ingestion

Raw data acquisition and ingestion lifecycle management for the Atlas Platform.

---

## Purpose

`atlas-ingestion` coordinates the acquisition of data from external sources into the raw (`bronze`) storage layer of the Atlas Platform. It sits between `atlas-orchestration` (which coordinates multi-stage workflows) and the lower-level `atlas-connectors` and `atlas-storage` packages.

---

## Responsibilities

- Validate ingestion requests and execution parameters (`IngestionRequest`, `ExecutionConfig`).
- Manage the discrete ingestion state machine (`IngestionLifecycle`, `IngestionState`).
- Track execution records, timestamps, state transitions, and failure diagnostics (`ExecutionTracker`, `IngestionExecutionRecord`) without coupling to a specific database.
- Resolve and invoke source connectors through `atlas-connectors` public interfaces (`Extractor`).
- Persist extracted payloads into the raw (`bronze`) layer through `atlas-storage` public interfaces (`Loader`).
- Produce standardized outcomes and output artifact references (`IngestionResult`).
- Expose a clean, dependency-injected public entry point (`IngestionService`) for `atlas-orchestration`.

`atlas-ingestion` does **not** own workflow scheduling, multi-step orchestration, REST APIs, silver/gold data transformations, or data quality rule evaluation.

---

## Architecture and Components

```text
atlas-orchestration
        |
        v
atlas-ingestion (IngestionService -> IngestionPipeline)
   |            |                  |
   v            v                  v
Extractor     Loader        ExecutionTracker
   |            |                  |
   v            v                  v
connectors   storage           atlas-core
    \          /
     atlas-core
```

| Component | Responsibility |
| --- | --- |
| `service.py` | Public ingestion service (`IngestionService`) consumed by `atlas-orchestration`. |
| `pipeline.py` | Coordinates validation, execution creation, lifecycle transitions, extraction, loading, and error handling (`IngestionPipeline`). |
| `lifecycle.py` | Enforces valid ingestion state transitions (`PENDING` → `EXTRACTING` → `EXTRACTED` → `LOADING` → `COMPLETED` / `FAILED`). |
| `exceptions.py` | Ingestion-specific exception hierarchy rooted at `atlas_core.exceptions.IngestionError`. |
| `models/ingestion_request.py` | Validated ingestion request (`IngestionRequest`) and execution options (`ExecutionConfig`). |
| `models/ingestion_result.py` | Standardized ingestion outcome (`IngestionResult`) extending `atlas_core.models.result.Result`. |
| `extraction/extractor.py` | Resolves connectors via `ConnectorFactory`/`ConnectorRegistry` and normalizes `ExtractionResult`. |
| `loading/loader.py` | Serializes extracted datasets and persists raw artifacts via `StorageManager`/`StorageRegistry`. |
| `tracking/execution_tracker.py` | Database-agnostic execution state tracker implementing `atlas_core.contracts.execution.ExecutionManager`. |

---

## Dependencies

- `atlas-core`: Shared contracts (`Connector`, `StorageProvider`, `ExecutionManager`), models (`Result`, `Execution`, `Artifact`, `Source`), enums (`ExecutionStatus`), identifiers, and exceptions.
- `atlas-connectors`: `ConnectorRegistry`, `ConnectorFactory`, `SourceConfig`, `ExtractionResult`, connectors, and format parsers.
- `atlas-storage`: `StorageRegistry`, `StorageManager`, `StorageLocation`, `StorageResult`, `PartitionStrategy`, and storage backends.

---

## Public Interfaces

All public types are exported directly from `atlas_ingestion`:

- **Service & Pipeline**: `IngestionService`, `IngestionPipeline`
- **Models**: `IngestionRequest`, `ExecutionConfig`, `IngestionResult`
- **Lifecycle**: `IngestionLifecycle`, `IngestionState`, `VALID_TRANSITIONS`, `TERMINAL_STATES`
- **Tracking**: `ExecutionTracker`, `IngestionExecutionRecord`
- **Extraction & Loading**: `Extractor`, `Loader`, `create_default_connector_registry`, `create_default_storage_registry`
- **Exceptions**: `IngestionError`, `InvalidIngestionRequestError`, `LifecycleTransitionError`, `ExecutionTrackingError`, `ExtractionError`, `LoadingError`

---

## Usage Example

```python
from atlas_connectors.models.source_config import SourceConfig
from atlas_core.constants import BRONZE
from atlas_core.types import DatasetId
from atlas_storage.models.storage_location import StorageLocation

from atlas_ingestion import (
    ExecutionConfig,
    IngestionRequest,
    IngestionService,
)

service = IngestionService()

request = IngestionRequest(
    dataset_id=DatasetId("weather_observations"),
    source=SourceConfig(
        source_id="env-canada-csv",
        source_name="Environment Canada Daily Observations",
        source_type="local_file",
        config={"path": "data/incoming/weather.csv"},
    ),
    destination=StorageLocation(
        layer=BRONZE,
        path="data/bronze/weather/observations.parquet",
        format="parquet",
    ),
    source_format="csv",
    execution_config=ExecutionConfig(
        validate_connection=True,
        partition_by_date=True,
        overwrite=True,
    ),
)

result = service.ingest(request)

if result.success:
    print(f"Execution {result.execution_id} succeeded ({result.row_count} rows)")
    for artifact in result.artifacts:
        print(f"Artifact written to: {artifact.path}")
else:
    print(f"Execution {result.execution_id} failed: {result.error_message}")
```

---

## Configuration Example

```python
from atlas_connectors.files.local import LocalFileConnector
from atlas_connectors.registry import ConnectorRegistry
from atlas_storage.backends.filesystem import FileSystemBackend
from atlas_storage.registry import StorageRegistry

from atlas_ingestion import ExecutionTracker, IngestionService

connector_registry = ConnectorRegistry()
connector_registry.register("local_file", LocalFileConnector)

storage_registry = StorageRegistry()
storage_registry.register("filesystem", FileSystemBackend)

tracker = ExecutionTracker()

service = IngestionService(
    connector_registry=connector_registry,
    storage_registry=storage_registry,
    tracker=tracker,
)
```

---

## Testing

Run unit and integration tests for `atlas-ingestion` from the repository root:

```bash
# Unit tests
pytest modules/atlas-ingestion/tests/unit

# Integration tests
pytest modules/atlas-ingestion/tests/integration

# Linting and type checking
ruff check modules/atlas-ingestion
mypy modules/atlas-ingestion/src modules/atlas-ingestion/tests
```
