# atlas-processing

Reusable, engine-abstracted dataset transformation module for the Atlas Platform.

---

## Purpose

`atlas-processing` provides configurable data transformation capabilities that transform datasets from raw (`bronze`) or intermediate (`silver`) storage locations into curated (`silver` / `gold`) datasets. It separates declarative transformation definitions (`FilterTransformation`, `MapTransformation`, `AggregateTransformation`) from the underlying execution engine (`ProcessingEngine`, initially implemented via `PandasProcessingEngine`).

---

## Architecture

```text
             atlas-orchestration
                     |
                     v
              atlas-processing (ProcessingService -> ProcessingPipeline)
                     |
          +----------+----------+------------------+
          |          |          |                  |
          v          v          v                  v
       Storage   Executor    Storage        ExecutionTracker
       (Read)       |        (Write)               |
          |         v           |                  v
          |    ProcessingEngine |             atlas-core
          |    + Registry       |
          +---------+-----------+
                    |
                    v
             atlas-storage
                    |
                    v
               atlas-core
```

| Component | Responsibility |
| --- | --- |
| `service.py` | Public entry point (`ProcessingService`) exposing `process()`, `get_execution()`, and `list_executions()`. |
| `engine.py` | Execution engine contract (`ProcessingEngine`) and initial `PandasProcessingEngine` implementation. |
| `pipeline.py` | Coordinates request validation, execution tracking, storage reads, transformation execution, and curated writes (`ProcessingPipeline`). |
| `exceptions.py` | Processing-specific exception hierarchy rooted at `atlas_core.ProcessingError`. |
| `models/processing_request.py` | Validated processing request (`ProcessingRequest`) and execution options (`ProcessingExecutionConfig`). |
| `models/processing_result.py` | Standardized processing outcome (`ProcessingResult`) and summary metrics (`ProcessingStatistics`). |
| `transformations/base.py` | Common `Transformation` contract (implementing `atlas_core.Processor`) and declarative `TransformationStep`. |
| `transformations/registry.py` | Registers and resolves transformations (`TransformationRegistry`), preventing accidental duplicates. |
| `transformations/filtering.py` | Configurable single and multi-condition row filtering (`FilterTransformation`, `FilterCondition`, `FilterOperator`). |
| `transformations/mapping.py` | Column renaming, projection, dropping, derived columns, and operations (`MapTransformation`, `DerivedColumn`). |
| `transformations/aggregation.py` | Configurable grouping and aggregation (`AggregateTransformation`, `AggregationSpec`, `AggregationFunction`). |
| `execution/executor.py` | Sequential transformation step runner (`TransformationExecutor`) recording step-level metrics. |
| `tracking/execution_tracker.py` | Lifecycle state machine (`ProcessingLifecycle`, `ProcessingState`) and in-memory `ExecutionTracker`. |

---

## Dependencies

- `atlas-core`: Shared contracts (`Processor`, `ExecutionManager`, `StorageProvider`), domain models (`Result`, `Execution`, `Artifact`), enums (`ExecutionStatus`), and exceptions.
- `atlas-storage`: `StorageManager`, `StorageRegistry`, `StorageLocation`, `PartitionStrategy`, and format serializers.

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

service = ProcessingService()

request = ProcessingRequest(
    dataset_id=DatasetId("weather_observations"),
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
mypy modules/atlas-processing/src modules/atlas-processing/tests
```
