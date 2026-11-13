# atlas-core

Shared platform primitives, domain models, canonical URN identity utilities, and service contracts used across Atlas modules.

## Components

| Order | File | Responsibility |
| --- | --- | --- |
| 1 | `exceptions.py` | Platform exception hierarchy |
| 2 | `identifiers.py` | Dataset, artifact, and execution ID generators plus canonical URN utilities (`build_urn`, `build_dataset_urn`, `build_artifact_urn`, `is_canonical_urn`) |
| 3 | `enums.py` | Execution status and lifecycle enums |
| 4 | `models/result.py` | Standard operation result |
| 5 | `models/dataset.py` | Core `Dataset` model (including `urn`, `namespace`, `tags`, `upstream_urns`, `metadata`) |
| 6 | `models/execution.py` | Execution state and provenance model |
| 7 | `models/source.py` | Data source definition |
| 8 | `models/artifact.py` | Physical data `Artifact` definition (including `urn`, `layer`, `execution_id`, `row_count`, `byte_size`, `tags`) |
| 9 | `models/quality.py` | Quality validation result |
| 10 | `settings.py` | Environment-aware configuration management (`AtlasSettings`) |
| 11 | `logging.py` | Structured logging (`configure_logging`, `get_logger`) |
| 12 | `contracts/connector.py` | Data source `Connector` contract |
| 13 | `contracts/storage.py` | Physical `StorageProvider` contract |
| 14 | `contracts/processor.py` | Data transformation `Processor` contract |
| 15 | `contracts/quality.py` | `QualityRule` contract |
| 16 | `contracts/execution.py` | `ExecutionManager` contract |
| 17 | `contracts/metadata.py` | `MetadataProvider` contract (`get_dataset`, `register_dataset`, `register_dataset_metadata`, `register_artifact`) |

## Canonical URN Utilities

```python
from atlas_core import build_artifact_urn, build_dataset_urn, is_canonical_urn

dataset_urn = build_dataset_urn("orders_silver", namespace="commerce")
# "urn:atlas:dataset:commerce:orders_silver"

artifact_urn = build_artifact_urn("art-001", namespace="commerce")
# "urn:atlas:artifact:commerce:art-001"

assert is_canonical_urn(dataset_urn, entity_type="dataset") is True
```

## Exceptions

```python
raise ConfigurationError("Missing required source configuration.")

raise ConnectorError("Failed to retrieve Environment Canada dataset.")

raise ValidationError("Dataset schema does not match expected contract.")
```

## Logging

```python
from atlas_core.logging import configure_logging, get_logger

configure_logging()

logger = get_logger(
    "atlas.ingestion",
    source="environment_organization",
)

logger.info(
    "ingestion_started",
    dataset="weather_observations",
)

logger.warning(
    "quality_check_failed",
    row_count=25,
)
```

## Settings

```python
from atlas_core.settings import settings

print(settings.environment)
print(settings.log_level)
```
