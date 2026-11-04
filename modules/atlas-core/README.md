# atlas-core

Shared platform primitives used across Atlas modules.

## componenets


| Order | File | Responsibility |
|---------|---------|--------------|
| 1 | `exceptions.py` | Platform exception hierarchy |
| 2 | `identifiers.py` | Dataset, job, and execution identifiers |
| 3 | `enums.py` | Execution status and artifact types |
| 4 | `models/result.py` | Standard operation result |
| 5 | `models/dataset.py` | Dataset definition |
| 6 | `models/execution.py` | Execution state and information |
| 7 | `models/source.py` | Source definition |
| 8 | `models/artifact.py` | Data artifact definition |
| 9 | `models/quality.py` | Quality validation result |
| 10 | `settings.py` | Configuration management |
| 11 | `logging.py` | Structured logging |
| 12 | `contracts/connector.py` | Data source connector contract |
| 13 | `contracts/storage.py` | Storage provider contract |
| 14 | `contracts/processor.py` | Processing contract |
| 15 | `contracts/quality.py` | Quality rule contract |
| 16 | `contracts/execution.py` | Execution management contract |
| 17 | `contracts/metadata.py` | Metadata management contract |


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

Example:

```text
.env.local

ATLAS_LOG_LEVEL=DEBUG
ATLAS_STORAGE_ROOT=data
```

```python
# .env.environment
local
```

`AtlasSettings` automatically loads the correct environment configuration.
