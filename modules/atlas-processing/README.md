# atlas-processing

Reusable, engine-abstracted dataset transformation module for the Atlas Platform.

---

## Purpose

`atlas-processing` provides configurable data transformation capabilities that transform datasets from raw (`bronze`) or intermediate (`silver`) storage locations into curated (`silver` / `gold`) datasets. It separates declarative transformation definitions (`FilterTransformation`, `MapTransformation`, `AggregateTransformation`) from the underlying execution engine (`ProcessingEngine`, initially implemented via `PandasProcessingEngine`), while optionally registering curated datasets, canonical upstream URN lineage, and output artifacts through the `atlas_core.MetadataProvider` contract.

---

## Architecture

```text
              atlas-orchestration
                      |
                      v
               atlas-processing (ProcessingService -> ProcessingPipeline)
                      |
      +---------------+---------------+------------------+------------------+
      |               |               |                  |                  |
      v               v               v                  v                  v
   Storage         Executor        Storage        MetadataProvider   ExecutionTracker
   (Read)             |            (Write)         (Optional DI)            |
      |               v               |                  |                  v
      |        ProcessingEngine       |                  |              atlas-core
      |        + Registry             |                  |
      +---------------+---------------+                  |
                      |                                  v
                      v                             atlas-core
               atlas-storage                     (Dataset / Artifact)
                      |
                      v
                 atlas-core
```

| Component | Responsibility |
| --- | --- |
| `service.py` | Public entry point (`ProcessingService`) exposing `process()`, `get_execution()`, and `list_executions()`, with optional `metadata_provider: MetadataProvider | None` injection. |
| `engine.py` | Execution engine contract (`ProcessingEngine`) and initial `PandasProcessingEngine` implementation. |
| `pipeline.py` | Coordinates request validation, execution tracking, storage reads, transformation execution, curated writes, and independent dataset/artifact metadata registration (`ProcessingPipeline`). |
| `exceptions.py` | Processing-specific exception hierarchy rooted at `atlas_core.ProcessingError`. |
| `models/processing_request.py` | Validated processing request (`ProcessingRequest`) supporting `source_dataset_id`, `namespace`, `tags`, `upstream_urns`, and execution options (`ProcessingExecutionConfig`). |
| `models/processing_result.py` | Standardized processing outcome (`ProcessingResult`) and summary metrics (`ProcessingStatistics`). |
| `transformations/base.py` | Common `Transformation` contract (implementing `atlas_core.Processor`) and declarative `TransformationStep`. |
| `transformations/registry.py` | Registers and resolves transformations (`TransformationRegistry`), preventing accidental duplicates. |
| `transformations/filtering.py` | Configurable single and multi-condition row filtering (`FilterTransformation`, `FilterCondition`, `FilterOperator`). |
| `transformations/mapping.py` | Column renaming, projection, dropping, derived columns, and operations (`MapTransformation`, `DerivedColumn`). |
| `transformations/aggregation.py` | Configurable grouping and aggregation (`AggregateTransformation`, `AggregationSpec`, `AggregationFunction`). |
| `execution/executor.py` | Sequential transformation step runner (`TransformationExecutor`) recording step-level metrics. |
| `tracking/execution_tracker.py` | Lifecycle state machine (`ProcessingLifecycle`, `ProcessingState`) and in-memory `ExecutionTracker`. |

---

## Metadata Registration & Lineage Semantics

When a `MetadataProvider` is injected into `ProcessingService` or `ProcessingPipeline`:

1. **Lineage Normalization (`source_dataset_id` + `upstream_urns`)**:
   - Combines both `source_dataset_id` (or the inferred source dataset ID) and any entries declared in `upstream_urns`.
   - Resolves each upstream reference via `metadata_provider.get_dataset(...)` when registered; if unregistered, constructs its canonical URN (`urn:atlas:dataset:<namespace>:<id>`) using `atlas_core.build_dataset_urn`.
   - Normalizes all upstream references to canonical URNs (never raw dataset IDs) and deduplicates entries in declaration order without losing any explicitly declared lineage.
2. **Namespace Assumption**:
   - Fallback URN construction follows `atlas-core` conventions (`DEFAULT_NAMESPACE = "default"`) unless `ProcessingRequest.namespace` (or `metadata["namespace"]`) is explicitly set, or a registered source dataset provides its own namespace.
   - Workspace filesystem paths and tenant identifiers are not coupled to metadata namespaces.
3. **Dataset Updates & Artifact History Preservation**:
   - **Dataset Catalog Record**: Registered via `metadata_provider.register_dataset_metadata(..., overwrite=True)` (keyed by `dataset_id`) so subsequent runs update the current dataset catalog entry (`upstream_urns`, `layer`, `format`, `path`, `tags`).
   - **Artifact History**: Each processing execution generates a unique `artifact_id` (`generate_artifact_id()`) and a distinct canonical artifact URN (`urn:atlas:artifact:<namespace>:<artifact_id>`), reusing the exact `execution_id` managed by `ExecutionTracker` and populating fields from `StorageResult` and destination `StorageLocation`. Because artifacts in the repository are keyed by `artifact_id` / `urn` rather than `dataset_id`, calling `register_artifact(..., overwrite=True)` is idempotent for that run's artifact and never overwrites historical artifacts from previous executions of the same dataset.
4. **Non-Transactional Boundary & Independent Failure Handling**:
   - Physical storage persistence (`StorageManager.save`) and metadata catalog registration (`MetadataProvider`) are **not transactional**.
   - Dataset registration and artifact registration execute in independent `try/except` blocks after physical storage write succeeds.
   - If either metadata registration fails, an actionable warning identifying `dataset_id`, `artifact_id`, and `execution_id` is logged (and recorded in `result.metadata`), while the persisted processing execution still completes with `ExecutionStatus.SUCCEEDED`.

---

## Dependencies

- `atlas-core`: Shared contracts (`Processor`, `ExecutionManager`, `StorageProvider`, `MetadataProvider`), domain models (`Result`, `Execution`, `Dataset`, `Artifact`), canonical URN helpers (`build_dataset_urn`, `build_artifact_urn`, `is_canonical_urn`), enums (`ExecutionStatus`), and exceptions.
- `atlas-storage`: `StorageManager`, `StorageRegistry`, `StorageLocation`, `StorageResult`, `PartitionStrategy`, and format handlers.

*(Note: `atlas-processing` depends only on `atlas-core` contracts and never imports `atlas-metadata` directly.)*

---

## Usage Example

```python
from atlas_core import BRONZE, SILVER, DatasetId
from atlas_storage import StorageLocation

from atlas_processing import (
    ProcessingExecutionConfig,
    ProcessingRequest,
    ProcessingService,
    TransformationStep,
)

# Optional: pass any atlas_core.MetadataProvider implementation
service = ProcessingService(metadata_provider=None)

request = ProcessingRequest(
    dataset_id=DatasetId("weather_observations_silver"),
    source_dataset_id=DatasetId("weather_observations_bronze"),
    namespace="climate",
    source=StorageLocation(
        layer=BRONZE,
        path="data/bronze/weather.csv",
        format="csv",
    ),
    destination=StorageLocation(
        layer=SILVER,
        path="data/silver/weather_curated.parquet",
        format="parquet",
    ),
    tags=["curated", "weather"],
    transformations=[
        TransformationStep(
            transformation_id="filter",
            params={
                "conditions": [
                    {"column": "temperature_c", "operator": "not_null"},
                    {"column": "temperature_c", "operator": "gte", "value": -50.0},
                ],
            },
        ),
        TransformationStep(
            transformation_id="map",
            params={
                "rename_columns": {"station_id": "station"},
                "operations": {"city": "upper"},
                "select_columns": ["station", "city", "temperature_c"],
            },
        ),
    ],
    execution_config=ProcessingExecutionConfig(
        overwrite=True,
        partition_by_date=True,
    ),
)

result = service.process(request)
```

---

## Testing

```bash
# Unit tests
pytest modules/atlas-processing/tests/unit

# Integration tests
pytest modules/atlas-processing/tests/integration

# Linting and type checking
ruff check modules/atlas-processing
mypy --strict modules/atlas-processing/src modules/atlas-processing/tests
```
