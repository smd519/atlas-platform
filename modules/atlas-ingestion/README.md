# atlas-ingestion

Raw data acquisition, execution-specific remote file staging, and ingestion lifecycle management for the Atlas Platform.

---

## Purpose

`atlas-ingestion` coordinates the acquisition of data from external or local sources into the raw (`bronze`) storage layer of the Atlas Platform. It sits between `atlas-orchestration` (which coordinates multi-stage workflows) and the lower-level `atlas-connectors` and `atlas-storage` packages.

---

## Responsibilities

- Validate ingestion requests and execution parameters (`IngestionRequest`, `ExecutionConfig`).
- Manage the discrete ingestion state machine (`IngestionLifecycle`, `IngestionState`).
- Track execution records, timestamps, state transitions, and failure diagnostics (`ExecutionTracker`, `IngestionExecutionRecord`) without coupling to a specific database.
- Create execution-specific staging directories (`<workspace>/drop/.staging/<execution_id>/`) for remote file downloads before invoking connectors.
- Resolve and invoke source connectors through `atlas-connectors` public interfaces (`Extractor`), passing `destination=staging_path` for remote file acquisitions.
- Parse local or staged files (`csv`, `json`, `parquet`, `raw`/`bytes`) and persist datasets into the raw (`bronze`) layer through `atlas-storage` public interfaces (`Loader`).
- Deterministically clean up execution-specific staging directories after Bronze persistence succeeds while preserving diagnostic staging artifacts when loading fails.
- Produce standardized outcomes and output artifact references (`IngestionResult`).
- Expose a clean, dependency-injected public entry point (`IngestionService`) for `atlas-orchestration`.

`atlas-ingestion` does **not** own workflow scheduling, multi-step orchestration, HTTP streaming/chunking mechanics, silver/gold data transformations, or data quality rule evaluation.

---

## Architecture and Components

```text
                 Workflow YAML
                      |
                      v
                atlas-ingestion
                      |
              Create staging path
                      |
                      v
              drop/.staging/
                <execution_id>/
                      |
                      v
              Connector.extract()
                 /          \
                /            \
       LocalFileConnector   RestConnector
                |            |
                |        HTTP streaming
                |        Atomic download
                |            |
                +------+-----+
                       |
                  Local Path
                       |
                       v
                   Extractor
                       |
                       v
                    Loader
                       |
                       v
                  atlas-storage
                       |
                       v
                     BRONZE
```

| Component | Responsibility |
| --- | --- |
| `service.py` | Public ingestion service (`IngestionService`) consumed by `atlas-orchestration`. |
| `pipeline.py` | Coordinates validation, execution creation, lifecycle transitions, extraction, Bronze loading, staging cleanup, and error handling (`IngestionPipeline`). |
| `lifecycle.py` | Enforces valid ingestion state transitions (`PENDING` → `EXTRACTING` → `EXTRACTED` → `LOADING` → `COMPLETED` / `FAILED`). |
| `exceptions.py` | Ingestion-specific exception hierarchy rooted at `atlas_core.exceptions.IngestionError`. |
| `models/ingestion_request.py` | Validated ingestion request (`IngestionRequest`) and execution options (`ExecutionConfig`). |
| `models/ingestion_result.py` | Standardized ingestion outcome (`IngestionResult`) extending `atlas_core.models.result.Result`. |
| `extraction/extractor.py` | Resolves connectors via `ConnectorFactory`/`ConnectorRegistry`, manages `<workspace>/drop/.staging/<execution_id>/` destination paths for remote files, and normalizes `ExtractionResult`. |
| `loading/loader.py` | Serializes extracted datasets and persists raw artifacts via `StorageManager`/`StorageRegistry`. |
| `tracking/execution_tracker.py` | Database-agnostic execution state tracker implementing `atlas_core.contracts.execution.ExecutionManager`. |

---

## Staging & Cleanup Lifecycle

- **Remote File Downloads (`source_type: rest` with `response_mode: file` or non-JSON file formats)**:
  1. `Extractor` resolves the workspace root (`ExecutionConfig.workspace`, request metadata/config, `ATLAS_WORKSPACE`, or destination workspace boundary) and creates `<workspace>/drop/.staging/<execution_id>/` with `mkdir(parents=True, exist_ok=True)`.
  2. `Extractor` invokes `connector.extract(destination=staging_file)`. `RestConnector` streams the HTTP response to a `.part` file and atomically renames it to `<workspace>/drop/.staging/<execution_id>/<download_filename>`.
  3. `Extractor` parses the completed local file and `Loader` writes the dataset to Bronze storage.
  4. **On Success**: Only after Bronze persistence succeeds, `IngestionPipeline` removes `<workspace>/drop/.staging/<execution_id>/`. Any cleanup error is logged and recorded in `result.metadata["staging_cleanup_error"]` without masking or failing the successful `IngestionResult`. Other executions' staging directories are never touched.
  5. **On Failure**:
     - If the HTTP download fails mid-stream, incomplete `.part` files are removed.
     - If extraction succeeds but Bronze loading fails, the downloaded file in `<workspace>/drop/.staging/<execution_id>/<download_filename>` is preserved for operator diagnostics and its path is recorded in `result.metadata["staging_path"]`.
- **Local File Sources (`source_type: local_file`)**:
  - Read directly from their configured path without creating staging directories and without copying, moving, or deleting the original source file.

---

## Source Configuration Examples

### Remote HTTP File Download (`rest`)

```yaml
source:
  source_id: corporations_cbca_csv
  source_name: Corporations Active CBCA CSV
  source_type: rest
  config:
    base_url: https://example.com
    endpoint: /downloads/corporations-active-cbca-en.csv
    response_mode: file
    download_filename: corporations-active-cbca-en.csv
```

### Local File (`local_file`)

```yaml
source:
  source_id: corporations_cbca_local
  source_name: Corporations Active CBCA CSV
  source_type: local_file
  config:
    path: drop/corporations-active-cbca-en.csv
```

---

## Python Usage Example

```python
from atlas_connectors import SourceConfig
from atlas_core import BRONZE, DatasetId
from atlas_storage import StorageLocation

from atlas_ingestion import (
    ExecutionConfig,
    IngestionRequest,
    IngestionService,
)

service = IngestionService()

request = IngestionRequest(
    dataset_id=DatasetId("corporations_cbca"),
    source=SourceConfig(
        source_id="corporations_cbca_csv",
        source_name="Corporations Active CBCA CSV",
        source_type="rest",
        config={
            "base_url": "https://example.com",
            "endpoint": "/downloads/corporations-active-cbca-en.csv",
            "response_mode": "file",
            "download_filename": "corporations-active-cbca-en.csv",
        },
    ),
    destination=StorageLocation(
        layer=BRONZE,
        path="data/bronze/corporations_cbca.parquet",
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
mypy --strict modules/atlas-ingestion/src
```
